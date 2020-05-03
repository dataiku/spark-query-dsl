# Spark Query DSL

This library provides a DSL (Domain Specific Language) to join
Spark DataFrames automatically or semi-automatically.

If your tables / DataFrames look like that:

```
             +-----+               +-----+                +------+ 
             | b   |               | c   |                | f    | 
             +-----+               +-----+                +------+ 
 +-----+     | id  |            +--+ id  |                | f_id | 
 | a   |  +--+ a   |            |  | ... |             +--+ e_id | 
 +-----+  |  | ... |  +------+  |  +-----+   +------+  |  | ...  | 
 | id  +--+  +-----+  | a_c  |  |            | e    |  |  +------+ 
 | ... |  |           +------+  |            +------+  |           
 |     |  +-----------+ a_fk |  |            | e_id +--+           
 +-----+              | c_fk +--+            | ...  |              
                      +------+               +------+              
```

Then joins become simple:

```scala
(a + b - (a_c + c)) * (e + f)
```


## Features

Wrap a Spark DataFrame into a Query with a name

```scala
val a = Query(df1, "a")
val b = Query(df2, "b")
```


### Joins

Automatic join with column names

- Each (sub)query has an alias
- Can join on left alias `left` if:
    - it has `id` (configurable), or `left_id`
    - and `right` has `left_id`, `left_fk` or `left`
    - or vice-versa (`left` has `right_id` mapping to `right.id`)
- Override possible

Supports all types of join

```scala
// inner join: authors & their topics
person + topic
// multiple joins: authors, their topics and their messages
person + topic + message
// anti join: people without any message
person - message
// left outer join: people, possibly with their messages
person % message
// left semi join: people that have messages (select only the person)
person ^ message
// full outer join
person %% message
// right outer join
person %> message
// cross join: each combination of 2 people (spin the bottle)
person * person
// selft-join is tricky, requires alias
message.alias("parent") + message.on("message_id" -> "parent_id")
```

You can also specify on which columns to join:

```scala
// Join on all common columns
a + ~b
InnerJoin(a, b, CommonColumnsJoiner)
// Specify columns to join on
a + b.on("col1", "col2", "col3")
// Specify different columns left & right
a + b.on("left_col1" -> "right_col1", ...)
// Specify spark.sql.Column join column
a + b.on(a("left_col1") === b("right_col1"))
```

And union:

```scala
a & b
```


### Filters

```scala
a | a("ok") === 1
a + b | a("ok") === 1
FilterQuery(a, a("ok") === 1)
FilterQuery(a, expr("ok == 1"))
```

Chain filters (and)

```scala
(a + b - c) * d   | (a("ok") === 1)   | (b("also_ok") === 1)
```

Disambiguate similar fields in filters:

```scala
person + topics | person("created") > topic("created")
```


### Selection

```scala
// topic with an author whose name finishes with an e
// but don't care about the author, just used for filtering
(topic + person | person("name").endsWith("e")).select(topic)

// select all leaves with auto-prefix
(topic + person).select()  // topic_title, person_name, ...

// select all with no prefix, skip duplicates
(topic + person).select(true)  // title, name, ...
```


### Group by

```scala
// person with count of distinct messages
(person % message) / person

// person with count of distinct messages & topics
(person % message % topic) / person
```


### Combinations

```scala
// union of untitled topic authors (left semi join)
// and people who have posted no messages
(person ^ (topic | topic("title").isNull))
  & (person - message)
```


## Copyright & license

Copyright (c) 2020 Adrien Lavoillotte, Dataiku

Distributed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0)
