---
layout: post
title: Production Ready Spam Classifier From Scratch
---

I recently had the opportinuity to solve an issue with spammers for our products. The products community sites with the ability for users to send messages to each others, and quite many of these messages are spam users creating accounts and sending bait messages to other users to make them visit certain sites or to get their email addresses. The previous solution has been a simple list of keywords that when used would automatically block the user. This solution has many drawbacks, since spammers can easily circumvent a keyword 'funsite.com' with 'f u n s i t e . c o m'. This has caused the list of keywords to grow and grow, year after year, and currently contains over a thousand of these keywords, and whenever a user sends a message, a check is made against this table, which is neither scalable nor accurate, so we needed a better solution.

The final implementation is an asynchronous streaming classifier using [Apache Spark](http://spark.apache.org/) running on [Apache Hadoop](http://hadoop.apache.org/) using Yarn, with incomming messages being read from an [Apache Kafka](http://kafka.apache.org/) cluster. The pre-trained models are stored in HDFS and each Spark worker downloads the models to their local storage upon startup and loads them into memory. The classifier's predictions are sent to a queue for the different communities to consume and decide what they want to do with the information given.

![architecture](/images/spam-hdfs.png)

Most of the information in this post is theory and not too much concrete examples and code, since I'm not at liberty to share everything.

The first part of this post is about how to manually label a large amount of unlabelled raw messages as either spam or ham to later be used as training data for the actual classifier which is described in part two. The third part will deal with how to run the classifier in a production environment consisting of Kafka, Zookeeper, HDFS and Spark. Finally, the fouth part will talk about how to visualize and evaluate the streaming classifier.

* This line is a placeholder to generate the table of contents
{:toc}

## Labelling training data

I will assume you already have a database dump of actual messages sent in your production environment.

For our purposes, the training data will reside in MySQL so we can more easily manage and improve the training data over time through a web interface. The table structure I'll be using looks like the following:

```sql
CREATE TABLE `training` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `class` int(11) NOT NULL,
  `message` text CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`id`),
  FULLTEXT KEY `message` (`message`)
) ENGINE=InnoDB AUTO_INCREMENT=50894 DEFAULT CHARSET=utf8 COLLATE=utf8_bin
```

We're using the InnoDB engine above instead of the MyISAM engine because we need the fulltext key on the `message` column to match some approximate string searches later to find likely spam messages. When you're confident that your training data is more or less correctly labelled you could switch the engine if need be.

In our case we have a way for users to report messages as spam, so we have a seperate dump for non-reported messages and reported messages. Since many users might have reported messages as spam that are actually not spam, this is of course not fool-proof, though it gives us a starting point; since most messages are not spam, if we'd just dump say 50k random messages there might be an unproportional amount of non-spam compared to spam, so we're dumping roughly 25k reported messages and 25k random messages.

You could of course use more messages for the training data, though using around 50k messages as training data gave us a resonable accuracy of 99.98% after some tweaking. Training on this amount of data and doing cross-validation consumed roughly 30 GB of memory, so if you have both the time and memory you could increase the size of the training data, at the expense of more time spending trying to correctly label everything.

Initially we set the class of all messages (included the reported ones) to 0 (non-spam, whereas 1 would indicate spam).

```sql
update training set class = 0;
```

Likely there are some duplicate messages in the training data right now, both from non-spam (many users would send short messages such as 'hi' and 'how are you?') and spam (since spammers usually send the same message to multiple people). Though the accuracy of the classifier won't be affected much, having duplicates would increase the training time, so we might as well remove them:

```sql
delete train1 from training train1, training train2 
where 
    train1.id > train2.id and 
    train1.message = train2.message;
```

Secondly, in most spam messages we've seen, there are links they want others to follow, so initially we could label all messages with easy-to-find links as spam (in our use case we mostly don't want users to send links anyway, whether legit or spam, but this might not be the same in your case):

```sql
update training
    set class = 1 
where 
    (message like '%.com %' or 
    message like '%.net %' or 
    message like '%.de %') and 
    message not like '%your-domain.de %' and 
    message not like '%your-other-domain.com%' and 
    message not like '%youtube.com%' and
    message not like '%facebook.com%' and
    class = 0;
```

Notice the space at the end of 'your-domain.de '; this is because our messages are in german, and surprisingly often users don't use a space after a full-stop, meaning the `like` query could potentially find sentences which are not actually links, since 'de' is a common prefix for many german words.

Many of our spam messages contains obfuscated linkes, such as `F˔ u˔ n˔ s˔ i˔ t˔ e. c˔ o˔ m` and `F˖u˖n˖s˖i˖t˖e . n˖e˖t!`, to try to circumvent simple keyword matching. Funsite is one domain that often shows up in these ways in our messages (not the real domain, funsite is used here to not disclose the actual spam domain used). Using regular expressions we can find quite a lot of these messages easily:

```sql
update training set class = 1 
where 
    message regexp 'F.{0,6}U.{0,6}N.{0,6}S.{0,6}I.{0,6}T.{0,6}E' 
    and class = 0;
```

Make sure you do a select before update so you're sure that the messages matching the regex are actually real spam messages.

After the above we can utilize the fulltext matching supported by InnoDB to do a fuzzy string search, finding messages similar to actual spam messages. First add the fulltext index to the table:

```sql
ALTER TABLE training ADD FULLTEXT index_name(message);
```

Then find the most matching messages:

```sql
select id, class, message, 
    match (message) against("Melde dich bitte  zuerst hier= Fˈ uˈ nˈ sˈ iˈ tˈ e . nˈ eˈ t!!") as score 
from training where 
    match (message) against("Melde dich bitte  zuerst hier= Fˈ uˈ nˈ sˈ iˈ tˈ e . nˈ eˈ t!!") and 
    class = 0
limit 50;
```

Change the above string to parts of other spam messages until you can't find any more messages still labelled as non-spam. This can be a tedious process, but it works surprisingly well to find these kinds of messages.

Next, we have many spam links that ends in two digits, for example `funsite18.net` and `coolplace21.com`. These links are usually obfuscated into something similar to `funsite 18++n++e+++t+++` or `coolplace 21 ...c,,,0...m...`. Using regular expressions we can find these as well:

```sql
select count(*) from training
where 
    message regexp '[0-9]{2}.{0,5}[nN].{0,3}[neNE3].{0,3}[tT]';

select count(*) from training 
where 
    message regexp '[0-9]{2}.{0,5}[cC].{0,3}[oO0].{0,3}[mM]';
```

Lastly, we can run the classifier on this and print all false positives. If you have most of your data labelled correctly, the classifier will most likely classify the messages you've missed to manually label as spam, while in the dataset they're still labelled as ham, giving you a list of false positives, of which most messages are likely to be messages you actually want to label as spam. For example during the evaluation phase:

```python
# y_valid is the labels from the training set, y_pred is what the 
# classifier predicted (% probability of being spam)
for index, (y_true, y_guess) in enumerate(zip(y_valid, y_pred)):
    # if the classifier is unsure likely it's not our spam messages
    if y_true == 0 and y_guess >= 0.7:
        # printing the messages from a copy of the input matrix since 
        # the one used for training contains the transformed messages 
        # which are not human-readable
        print('[guess: %s, true: %s]' % (y_guess, y_true))
        print(X_valid_original[index].encode('utf-8').strip())
```

Update the training data based on what the classifier finds. By splitting the data randomly to get both training data and validation data, you'd have to run the classifier a couple of times to find the miss-labelled data in the different folds. If you validate on the completed set you might not find all messages anyway since the classifier will learn from the training set and might not find the wrongly classified messages on that set but only on the validation set.

## The classifier

### Offline training

The classifier is an [ensemble of multiple classifiers](https://en.wikipedia.org/wiki/Ensemble_learning), who's output is fed to a single [blender](http://www.chioka.in/stacking-blending-and-stacked-generalization/) (also called stacking in some literature) algorithm (logistic regression) to form the final prediction. [Scikit-learn](http://scikit-learn.org/stable/) is heavily used, as well as [TensorFlow](https://www.tensorflow.org/) and [XGBoost](https://github.com/dmlc/xgboost).

First we need to preprocess the data; here we're using a short pipeline consisting of a [stemmer](https://en.wikipedia.org/wiki/Stemming), [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) [vectorizer](https://en.wikipedia.org/wiki/Vector_space_model) and a dense transformer (since some of the classifiers don't work on [sparse data](https://en.wikipedia.org/wiki/Sparse_matrix)):

```python
transformers = {
    TFIDF: StemmedTfidfVectorizer(stemmer, min_df=1, stop_words='german', decode_error='ignore'),
    DENSE: DenseTransformer()
}
```

The stemmer is combined with the TF-IDF vectorizer using a custom class:

```python
class StemmedTfidfVectorizer(TfidfVectorizer):
    def __init__(self, stemmer, **args):
        super(StemmedTfidfVectorizer, self).__init__(args)
        self.stemmer = stemmer

    def build_analyzer(self):
        analyzer = super(TfidfVectorizer, self).build_analyzer()
        return lambda doc: (self.stemmer.stem(w) for w in analyzer(doc))
```

And the DenseTransformer looks like this:

```python
class DenseTransformer(TransformerMixin):
    def transform(self, X, y=None, **fit_params):
        return X.todense()

    def fit_transform(self, X, y=None, **fit_params):
        self.fit(X, y, **fit_params)
        return self.transform(X)

    def fit(self, X, y=None, **fit_params):
        return self
```

Vectorization (or more specifically a Vector Space Model) is a technique for representing text as numerical vectors, which is useful for us since classification algorithms mostly operate on numerical data. TF-IDF stands for Term Frequency, Inverse Document Frequency, which is a way to find relevant words which are characteristic to the document at hand relative to the complete corpus. For an introduction on how VSM and TF-IDF works I would refer to Christian Perone's introduction, both [part 1](http://blog.christianperone.com/2011/09/machine-learning-text-feature-extraction-tf-idf-part-i/) and [part 2](http://blog.christianperone.com/2011/09/machine-learning-text-feature-extraction-tf-idf-part-ii/).

For the classifiers, the following 8 classifiers were chosen for the first layer (the parameters have been chosen by [cross-validation](https://en.wikipedia.org/wiki/Cross-validation_%28statistics%29) using scikit-learn's GridSearchCV):

```python
classifiers = {
    BAYES: MultinomialNB(alpha=0.02),
    LOG_SAG: LogisticRegression(penalty='l2', solver='sag', C=100, max_iter=250, n_jobs=-1),
    TENSOR_DNN: TensorFlowDNNClassifier(hidden_units=[10, 20, 10], n_classes=2, steps=1600, learning_rate=0.08, optimizer='Adagrad', verbose=0),
    FOREST: RandomForestClassifier(n_estimators=500, max_depth=100, verbose=0, n_jobs=-1, oob_score=True),
    SVC: LinearSVC(penalty="l1", dual=False, tol=1e-3),
    PERCEPTRON: Perceptron(n_iter=50),
    PASSIVE_AGGRESSIVE: PassiveAggressiveClassifier(n_iter=25, C=0.05, n_jobs=-1),
    XGBOOST: XGBoostClassifier(eval_metric='auc', num_class=2, nthread=8, eta=0.1, num_boost_round=100, max_depth=6, subsample=0.6, colsample_bytree=1.0, silent=1),
}
```

The second layer of the classifier is the blender, which will predict the true class from what the classifiers output:

```python
blenders = {
    BLEND_LOG_LINEAR: LogisticRegression(C=15, penalty='l2', max_iter=150, n_jobs=-1, solver='sag'),
}
```

Here we could add multiple blenders and use majority voting as the third layer, though after testing this did not increase the accuracy enough to justify the added complexity. Visually it looks like this:

![spam-levels](/images/spam-levels.png)

Now we can start to train the classifiers (if you have enough memory you could train multiple, or all, in parallell by increasing the number of training threads):

```python
@staticmethod
def fit(X, y):
    X = Classifier.pipeline.fit_transform(X, y)
    running_threads = Queue()
    waiting_threads = []

    for index, (name, clf) in enumerate(Classifier.clfs):
        thread = ThreadedTrain(name, clf, X, y, index)
        if running_threads.qsize() < TRAINING_THREADS:
            running_threads.put((name, thread))
            thread.start()
        else:
            waiting_threads.append((name, thread))

    done = 0
    loop_threads = list()

    while done < len(Classifier.clfs):
        while not running_threads.empty():
            loop_threads.append(running_threads.get())

        for clf_id, thread in loop_threads:
            thread.join()
            if thread.done and not thread.checked:
                done += 1
                thread.checked = True

                if len(waiting_threads) > 0:
                    (new_id, thread) = waiting_threads.pop()
                    running_threads.put((new_id, thread))
                    thread.start()
```

Or if you simply prefer to train them in sequence:

```python
@staticmethod
def fit(X, y):
    X = Classifier.pipeline.fit_transform(X, y)

    for index, (name, clf) in enumerate(Classifier.clfs):
        thread = ThreadedTrain(name, clf, X, y, index)
        thread.start()
        thread.join()
```

The `ThreadedTrain` class looks like this:

```python
class ThreadedTrain(threading.Thread):
    def __init__(self, clf_id, clf, X_train, y_train, index):
        super(ThreadedTrain, self).__init__()
        self.X_train = X_train
        self.y_train = y_train
        self.clf_id = clf_id
        self.clf = clf
        self.index = index
        self.done = False
        self.checked = False
        self.y_preds = None

    def run(self):
        start = time.time()
        self.clf.fit(self.X_train, self.y_train)
        self.done = True
        print('[%s] fitting took %.2fs' % (self.clf_id, time.time()-start))
```

Before we can begin training, we need to initialize the transformers, classifiers and blenders:

```python
@staticmethod
def train_classifier(data=None, validate=False):
    Classifier.set_transformers()
    Classifier.set_clfs()
    Classifier.set_bclfs()
    Classifier.train_model(data=data, validate=validate)
    Classifier.trained = True

@staticmethod
def set_clfs():
    Classifier.clfs = [
        (Classifier.BAYES, Classifier.classifiers[Classifier.BAYES]),
        (Classifier.LOG_SAG, Classifier.classifiers[Classifier.LOG_SAG]),
        (Classifier.TENSOR_DNN, Classifier.classifiers[Classifier.TENSOR_DNN]),
        (Classifier.FOREST, Classifier.classifiers[Classifier.FOREST]),
        (Classifier.SVC, Classifier.classifiers[Classifier.SVC]),
        (Classifier.PERCEPTRON, Classifier.classifiers[Classifier.PERCEPTRON]),
        (Classifier.PASSIVE_AGGRESSIVE, Classifier.classifiers[Classifier.PASSIVE_AGGRESSIVE]),
    ]

@staticmethod
def set_bclfs():
    Classifier.bclfs = [
        (Classifier.BLEND_LOG_LINEAR, Classifier.blenders[Classifier.BLEND_LOG_LINEAR]),
    ]

@staticmethod
def set_transformers():
    Classifier.pipeline = Pipeline([
        (Classifier.TFIDF, Classifier.transformers[Classifier.TFIDF]),
        (Classifier.DENSE, Classifier.transformers[Classifier.DENSE]),
    ])
```

The reason for setting `clfs`, `bclfs` and `pipeline` instead of just using `classifiers`, `blenders` and `transformers` directly is because when the classifier is running on Spark later, we want to replace these definitions with the pre-trained models that the trainer has saved to HDFS (more on this in the next section).

The `data` variable in the `train_classifier` method is simple a [pandas](http://pandas.pydata.org/) DataFrame:

```python
def load_data():
    table = config.get(ConfigKeys.JDBC_TABLE, 'training')
    cursor.execute("select class, message from %s" % table)
    _data = cursor.fetchall()

    X = np.array([d[1] for d in _data])
    y = np.array([d[0] for d in _data])
    del _data

    _data = pd.DataFrame({'0': y, '1': X})
    del X, y
    return _data

data = load_data()
```

Now we can finally begin training, see the `train_model` method below:

```python
@staticmethod
def train_model(data):
    y = data[data.columns[0]]
    X = data[data.columns[1]]
    y = np.array(y).astype(int)

    X_train, X_valid, y_train, y_valid = train_test_split(X, y)

    if not Classifier.trained:
        Classifier.fit_classifiers(X_train, y_train)

    X_blend = Classifier.validate_classifiers(Classifier.clfs, X_train, y_train)
    Classifier.fit_blenders(X_blend, y_train)

    X_blend = Classifier.validate_classifiers(Classifier.clfs, X_valid, y_valid)
    Classifier.validate_blenders(Classifier.bclfs, X_blend, y_valid)
```

Here we simply use the training set again for prediction, this output predictions is what's used as training data for the blenders. If you have enough data it would be better to split the data into three seperate sets; use the first set to fit the classifiers, use the second set to predict using the classifiers, and use those predictions as training data for the blenders, and finally using the third set to validate the blenders. In practice though did didn't have a big impact on the accuracy for us by doing it this way, it was still higher than not using a blender, and 50k samples were a bit too small of a dataset do be able to split into three sets. In the future when we have more labelled training data we would ideally switch over to use three sets during training.

A quick visualization of this three-way split: we would first train the classifier on the training split:

![training-split](/images/spam-split-1.png)

Then we validate the classifier using the second split, the 'blending' split, and use the predictions from the classifier as training data to the blender:

![blend-split](/images/spam-split-2.png)

Finally, we use the validation set to (again) validate the classifier and using the classifier's output as input to validate the blender:

![validation-split](/images/spam-split-3.png)

This way the blender gets to be trained on real training data that the classifier has not seen before. This makes the classifier train on one split and validate on two splits, though the output of the second split is only used as training data for the blender, then finally we can validate both the classifier's performance and the blender's performance on the third split, the validation data, which neither the classifier nor the blender has seen before.

Below is the `validate` and `validate_blenders` methods, which simply loops over the classifiers and aggregates the output:

```python
@staticmethod
def validate_classifiers(classifiers, X, y):
    return Classifier.validate(classifiers, Classifier.pipeline.transform(X), y)

@staticmethod
def validate_blenders(classifiers, X, y):
    return Classifier.validate(classifiers, to_blend_x(X), y)

@staticmethod
def validate(classifiers, X, y, X_copy=None):
    def validate_classifier(_classifier, _name, X_valid, y_valid):
        if hasattr(classifier, 'predict_proba'):
            y_hat = _classifier.predict_proba(X_valid)[:, 1]
        else:
            y_hat = _classifier.predict(X_valid)

        score(y_hat, y_valid, name=_name)
        return y_hat

    y_hats = dict()
    for index, (name, classifier) in enumerate(classifiers):
        y_hats[index] = validate_classifier(classifier, name, X, y)

    y_hat_ensemble = score_ensemble(y_hats, y, len(classifiers))
    print_validation(y, y_hat_ensemble, X_copy)

    return y_hats
```

I won't cover the `score` method, the only thing it does is printing some metrics.

Now we can combine all of this into a functional trainer:

```python
data = load_data()
Classifier.train_classifier(data)
```

When you're done with the cross-validation and you're happy with the accuracy you get, then instead of splitting the data into train and validation sets, retrain the whole model on one split of the training data, using the second split as training data for the blenders (by first using it as validation data for the classifiers).

After training we want to actually store these trained models on HDFS so our Spark workers can download and use them across the cluster:

```python
hdfs_path = config.get(ConfigKeys.HDFS_PATH, '/spam-models')
local_path = config.get(ConfigKeys.LOCAL_PATH, '/tmp/sf-models')

if not os.path.exists(local_path):
    os.makedirs(local_path)

# save models to hdfs so all workers can load them
hadoop = Hadoop(config.get(ConfigKeys.HADOOP_URL, 'http://localhost:50070'))
hadoop.delete(hdfs_path, recursive=True)
hadoop.upload(hdfs_path, local_path)
```

Above, `config` is a dictionary of configuration values read from Zookeeper, the distributed configuration system, since all workers need acess to the config values it makes sense to get them from somewhere else so we don't happen to have conflicting configurations spread out across the cluster. One way of getting these configuration values from Zookeeper is the following (env is a dictionary containing environment variables, either loaded from the yaml file or from variables set on the command line):

```python
def load_config(env):
    logger = logging.getLogger(__name__)
    zk_config_path = env.get('zk_config_path')
    zk_hosts = env.get('zk_hosts')

    zk = KazooClient(hosts=zk_hosts)
    zk.start()
    zk.ensure_path(zk_config_path)

    config = dict()
    for key, type in ConfigKeys.ALL:
        try:
            config[key] = type(zk.get('%s/%s' % (zk_config_path, key))[0])
        except NoNodeError as e:
            logger.error('could not get config key "%s" from zookeeper: %s' % 
                         (key, str(e)))
            raise e

    logger.info('read configuration from zookeeper: %s', config)
    return config
```

The keys `ConfigKeys.ALL` can look something like this:

```python
HADOOP_URL = 'hadoop-url'
HDFS_PATH = 'hdfs-path'
LOCAL_PATH = 'local-path'
JDBC_TABLE = 'jdbc-table'
JDBC_USER = 'jdbc-user'
JDBC_PASS = 'jdbc-pass'
JDBC_PORT = 'jdbc-port'
JDBC_HOST = 'jdbc-host'
JDBC_DB = 'jdbc-db'

ALL = [
    (HADOOP_URL, str),
    (HDFS_PATH, str),
    (LOCAL_PATH, str),
    (JDBC_TABLE, str),
    (JDBC_USER, str),
    (JDBC_PASS, str),
    (JDBC_PORT, int),
    (JDBC_HOST, str),
    (JDBC_DB, str),
]
```

Now that we have the pre-trained models stored in the distributed filesystem we can start looking a the Spark Streaming task that actually uses them.

### Streaming classification

Below is the code that starts the task. We're using a window length of 10 seconds, 5 consumers and 2 kafka partitions, repartitioned to 10 Spark partitions since we have 10 Spark workers in the cluster for spam classification:

```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

config, zk_hosts = load_config(env)

context = SparkContext(appName="StreamingSpamClassification")
stream = StreamingContext(context, config[ConfigKeys.WINDOW_LENGTH])

def create_kafka_streams():
    streaming_concurrency = config.get(ConfigKeys.STREAMING_CONCURRENCY, 5)
    kafka_partitions = config.get(ConfigKeys.KAFKA_PARTITIONS, 2)
    topic_name = config.get(ConfigKeys.TOPIC_NAME, 'chat-messages')
    return [
        KafkaUtils.createStream(stream, zk_hosts, "spam-consumer", 
                                {topic_name: kafka_partitions})
        for _ in range (streaming_concurrency)
    ]

spark_partitions = config.get(ConfigKeys.SPARK_PARTITIONS, 10)
stream.union(*create_kafka_streams())\
    .repartition(spark_partitions)\
    .foreachRDD(
        lambda rdd: rdd.foreachPartition(
                lambda partition: handle_partition(env, config, partition)))
```

The `handle_partition` method will run on the different Spark workers:

```python
def handle_partition(env, config, partition):
    download_models_from_hdfs(config)

    # load all models downloaded from hdfs into memory
    Classifier.load_classifier(config)

    # convert the itertools.chain into a list
    updates = list(partition)

    # a worker could get an empty partition
    if len(updates) == 0:
        return

    reject_max = config.get(ConfigKeys.REJECT_PROB_MAX)
    reject_min = config.get(ConfigKeys.REJECT_PROB_MIN)

    for k, update in updates:
        update = valid_json(update)
        if update is None:
            continue

        message, message_id = update['message'], update['message_id']
        probability_is_spam = Classifier.classify(message)

        if probability_is_spam > reject_max:
            notify_community(probability_is_spam, message_id)
        elif reject_min < probability_is_spam < reject_max:
            handle_rejected(probability_is_spam, message_id)

def download_models_from_hdfs(config):
    """
    If the models doesn't already exist on the local filesystem, downloaded them from HDFS.
    """
    local_path = config.get(ConfigKeys.LOCAL_PATH, '/tmp/sf-models/')
    hdfs_folder = config.get(ConfigKeys.HDFS_PATH, '/spam-models')

    if os.path.exists(local_path) and len(os.listdir(local_path)) > 0:
        return

    import shutil
    shutil.rmtree(local_path, ignore_errors=True)
    os.makedirs(local_path)
    hadoop = Hadoop(config.get(ConfigKeys.HADOOP_URL, 'http://localhost:50070'))
    hadoop.download(hdfs_folder, local_path)

    temp_hdfs_path = '%s/%s' % (local_path, hdfs_folder)
    model_files = os.listdir(temp_hdfs_path)
    for model_file in model_files:
        origin_path = '%s/%s' % (temp_hdfs_path, model_file)
        shutil.move(origin_path, local_path)
    shutil.rmtree(temp_hdfs_path)
```

{:.note}
> <h4>Reject Option</h4>
> An upper threshold can be configured for when to notify the communities. In our case we've set it to 70%, so any message that has a probability of being spam that is lower than 70% will be dropped (i.e. assumed to not be spam). Basically this is similar to the idea of having a [reject option](http://www.jmlr.org/papers/volume9/bartlett08a/bartlett08a.pdf) in a classifier. A reject option is when a binary classifier classifier something close to 50%, meaning it's completely unsure of which class the input belongs to, so we might as well flip a coin. Instead of flipping a coin the classifier then rejects the input, either drops it completely or let some human look at it and decide and based on this decision add the input to the training data. All of a sudden we have basic reinforcement learning.
> 
> In our case we save these rejected messages to a database (if the output is between 70% and 50%), which is rendered in the web interface to the classifier, so a human can look at it and decide whether or not these "possibly spam" messages are actually spam or not (more on this in the last part of this post). We have a quite high upper threshold for the reject option, since in spam classification you definitely don't want false positives (classifying a message as spam when it in fact wasn't), it's better for the users to sometimes let a real spam message through than to sometimes penalize users sending legitimate messages that happened to be classified as spam by the system. For recommendations this upper reject threshold could be much lower, since recommending something a users _might_ like isn't that bad.

The `valid_json` method above only checks whether or not the incoming message is valid json or not and contains the expected attributes.

Now the classifier will handle all messages posted to the Kafka cluster and notify the communities whenever it finds that a message is spam.

## Running in a production environment

When the clusters have been set up (zookeeper, kafka, hdfs, yarn), and the models have been trained and uploaded HDFS, we can run our Spark Streaming task with the help of YARN using a script somewhat similar to this:

```bash
VERSION=1.6.1
SPARK_HOME=/opt/spark/

$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:$VERSION \
    --py-files spam/environment.py,spam/classify.py,spam/spam.py,spam/xgboost_classifier.py,spam/utils.py \
    --master yarn spam/__init__.py -l DEBUG
```

Since we're using Kafka as an input source to Spark, we need to include the Kafka jar when submitting our Spark task.

Running this on our development environment and supplying the Kafka cluster in the development environment with a steady stream of test messages we can see the Spark metrics and resource allocation below (in our development environment both the Kafka and Hadoop clusters are virtual machines on the same physical node, the load visualization below is from that physical machine):

![spark-metrics](/images/spam-streaming-graphs.png)

![node-load](/images/spam-load.png)

## Visualization

TODO
