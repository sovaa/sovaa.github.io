---
layout: post
title: Spam Classifier Part 1 - Collecting training data
---

This is a first post in a four-part series on the theory behind a near real-time streaming text spam classifier for a messaging system. The first part is about how to manually label a large amount of unlabelled data as either spam or ham to be used as training data for the actual classifier described in part two of this series. The third part will deal with how to run the classifier in a production environment consisting of Kafka, Zookeeper, HDFS and Spark. Finally, the fouth part will talk about how to visualize and evaluate the streaming classifier.

I will assume you already have a database dump of actual messages sent in your production environment.

For our purposes, the training data will reside in MySQL so we can more easily manage and improve the training data over time through a web interface. The table structure I'll be using looks like the following:

```
CREATE TABLE `training` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `class` int(11) NOT NULL,
  `message` text CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`id`),
  FULLTEXT KEY `message` (`message`)
) ENGINE=InnoDB AUTO_INCREMENT=50894 DEFAULT CHARSET=utf8 COLLATE=utf8_bin
```

We're using the InnoDB engine above instead of the MyISAM engine because we need the fulltext key on the `message` column to match some approximate string searches later to find likely spam messages. When you're confident that your training data is more or less correctly labelled you could switch the engine if need be.

In our case we have a way for users to report messages as spam, so we have a seperate dump for non-reported messages and reported messages. Since many users might have reported messages as spam that are actually not spam, this is of course not fool-proof, though it gives us a starting point; since most messages are not spam, if we'd just dump say 50k random messages there might be an unproportional amount of non-spam compared to spam, so we're dump roughly 25k reported messages and 25k random messages.

You could of course use more messages for the training data, though using around 50k messages as training data gave us a resonable accuracy of 99.98 % after some tweaking. Training on this amount of data and doing cross-validation consumed roughly 30 GB of memory, so if you have both the time and memory you could increase the size of the training data, at the expense of more time spending trying to correctly label everything.

Initially we set the class of all messages (included the reported ones) to 0 (non-spam, whereas 1 would indicate spam).

```
update training set class = 0;
```

Likely there are some duplicate messages in the training data right now, both from non-spam (many users would send short messages such as 'hi' and 'how are you?) and spam (since spammers usually send the same message to multiple people). Though the accuracy of the classifier won't be affected much, having duplicates would increase the training time, so we might as well remove them:

```
delete train1 from training train1, training train2 where train1.id > train2.id and train1.message = train2.message;
```

Secondly, in most spam messages we've seen, there are links they want others to follow, so initially we could label all messages with easy-to-find links as spam (in our use case we mostly don't want users to send links anyway, whether legit or spam, but this might not be the same in your case):

```
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

Notice the space at the end of 'your-domain.de '; this is because our messages are in german, and often users don't use a space after a full-stop, meaning the like query could potentially find sentences which are not actually links, since 'de' is a common prefix for many german words.

Many of our spam messages contains obfuscated linkes, such as "F˔ u˔ n˔ s˔ i˔ t˔ e. c˔ o˔ m" and "F˖u˖n˖s˖i˖t˖e . n˖e˖t!", to try to circumvent simple keyword matching. Funsite is one domain that often shows up in these ways in our messages (not actually the domain, funsite is just used here to not disclose the actual spam domain used). Using regular expressions we can find quite a lot of these messages easily:

```
update training set class = 1 where message regexp 'F.{0,6}U.{0,6}N.{0,6}S.{0,6}I.{0,6}T.{0,6}E' and class = 0;
```

Make sure you do a select before update so you're sure that the messages matching the regex are actually real spam messages.

After the above we can utilize the fulltext matching supported by InnoDB to do a fuzzy string search, finding messages similar to actual spam messages. First add the fulltext index to the table:

```
ALTER TABLE training ADD FULLTEXT index_name(message);
```

Then find the most matching messages:

```
select id, class, message, 
    match (message) against("Melde dich bitte  zuerst hier= Fˈ uˈ nˈ sˈ iˈ tˈ e . nˈ eˈ t!!") as score 
from training where 
    match (message) against("Melde dich bitte  zuerst hier= Fˈ uˈ nˈ sˈ iˈ tˈ e . nˈ eˈ t!!") and 
    class = 0
limit 50;
```

Change the above string to parts of other spam messages until you can't find any more messages still labelled as non-spam. This can be a tedious process, but it works surprisingly well to find these kinds of messages.

Next, we have many spam links that ends in two digits, for example `funsite18.net` and `coolplace21.com`. These links are usually obfuscated into something similar to `funsite 18++n++e+++t+++` or `coolplace 21 ...c,,,0...m...`. Using regular expressions we can find these as well:

```
select count(*) from training where message regexp '[0-9]{2}.{0,5}[nN].{0,3}[neNE3].{0,3}[tT]';
select count(*) from training where message regexp '[0-9]{2}.{0,5}[cC].{0,3}[oO0].{0,3}[mM]';
```

Lastly, we can run the classifier on this and print all false positives. If you have most of your data labelled correctly, the classifier will most likely classify the messages you've missed to manually label as spam, while in the dataset they're still labelled as ham, giving you a list of false positives, of which most messages are likely to be messages you actually want to label as spam. For example during your evaluation phase:

```
# y_valid is the labels from the training set, y_pred is what the 
# classifier predicted (% probability of being spam)
for index, (y_true, y_guess) in enumerate(zip(y_valid, y_pred)):
    # if the classifier is unsure likely it's not our spam messages
    if y_true == 0 and y_guess >= 0.7:
        print('[guess: %s, true: %s]' % (y_guess, y_true))

        # printing the messages from a copy of the input matrix since 
        # the one used for training contains the transformed messages 
        # which are not human-readable
        print(X_valid_original[index].encode('utf-8').strip())

        print('==============\n')
```

Update the training data based on what the classifier finds. By splitting the data randomly to get both training data and validation data, you'd have to run the classifier a couple of times to find the miss-labelled data in the different folds. If you validate on the completed set you might not find all messages anyway since the classifier will learn from the training set and might not find the wrongly classified messages on that set but only on the validation set.

