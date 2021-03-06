---
title: Ruleset files
tags:
sidebar: mydoc_sidebar
permalink: ruleset.html
---

## Ruleset files

Comdb2 ruleset files have a `.ruleset` extension.  Ruleset files are optional.
By convention, ruleset files should be placed in the `rulesets` subdirectory
within the database directory.  If no ruleset files are loaded, the database
will assume all SQL queries have equal priority and they will be handled only
by its default thread pool.  By default, no ruleset files are loaded when the
database is started.  If desired, one (or more) ruleset files may be loaded
automatically on startup by using `do` directives in the
[LRL file](config_files.md#lrl-files), e.g.:

```
strict_double_quotes 1
do reload_ruleset /full/path/to/the/file.ruleset
```

In the above example, use of the `strict_double_quotes` tunable is optional;
however, it will permit loaded ruleset files to make use of the `fingerprint`
property.

A ruleset file consists of optional blank lines, optional comment lines, a
required file header, optional thread pool definition lines, and optional
rule definition lines.

Blank lines are skipped.  Lines beginning with `#` are treated as comments
and skipped.

The file header line must be the first non-blank, non-comment line in the
file.  It must consist of the literal string `version`, followed by one or
more spaces, followed by the file format version.  The currently valid file
format versions are `1` and `2`.

### Thread pool syntax (file format version 2 or later)

The syntax for thread pool definition lines is:

    pool <poolName> [attrName1 attrValue1] ... [attrNameN attrValueN]

A thread pool definition consists of a name and its associated attribute
values.  The thread pool name should be alphanumeric and cannot contain
any whitespace characters.  All attributes are optional; when absent, an
attribute will retain its system assigned default value.  These system
assigned default values are officially unspecified and may be changed at
any time.  The `threads` attribute is used to specify the maximum number
of threads for the thread pool.  It cannot be negative, nor can it exceed
the maximum number of threads used by the default thread pool.  Thread
pool definitions are optional.  When present, they must precede any rule
definitions that refer to them unless a matching rule has the `DYN_POOL`
flag set; otherwise, an error will be raised.

### Rule syntax

The syntax for rule definition lines is:

    rule <ruleNo> [propName1 propValue1] ... [propNameN propValueN]

A rule definition consists of an integer rule number and its associated
matching criteria.  The rule matching criteria consist of zero or more
property names and values.  The rule number must be an integer with a
value between one (1) and one thousand (1000).  Rules do not need to be
presented in numerical order within a ruleset file.  There may be rule
number gaps within a ruleset file (e.g. it is possible to define rules
1, 2, and 4 while skipping 3).  This can be useful if rulesets need to
be split into multiple files, e.g. to ease maintenance and reuse.  All
property names and values are optional; a line without at least one
property name and value does nothing except verify the rule number.
When a property name is specified its value must be specified as well.
Each property name may appear more than once for a particular rule, even
on the same line.  Only the last property value encountered for each
unique combination of rule number and property name will be retained.
Multiple ruleset files may be loaded.  As with a single ruleset file, in
the event duplicate rule numbers are encountered, only the last property
value encountered for each unique combination of rule number and property
name will be retained.  In general, the usage model for multiple ruleset
files is intended to have each ruleset file within a set restrict itself
to a particular range of rule numbers (e.g. `t1.ruleset` with rules 1 to
10, `t2.ruleset` with rules 11 to 20, etc).  Currently, the total number
of rules is limited to one thousand (1000).  All property values except
`fingerprint` are always interpreted based on the match mode configured
for the rule.  The `fingerprint` property value is always interpreted as
a sixteen (16) bytes to be compared exactly with the cryptographic hashes
calculated based on the [normalized](https://www.sqlite.org/c3ref/expanded_sql.html)
variants of submitted SQL queries.

### Property names and values

Property names are always delimited by whitespace.  All property values
except `flags`, `mode`, and `sql` are also delimited by whitespace.  The
special `flags`, `mode`, and `sql` property values are delimited by
semicolon because their values may contain whitespace.  The end-of-line
character (e.g. `\n`) always terminates a property value.

The supported set of property names and their required formats is:

| Property Name | Property Value Format |
|---------------|------------------------|
|action         | One of `NONE`, `REJECT_ALL`, `REJECT`, `UNREJECT`, `LOW_PRIO`, `HIGH_PRIO`, or `SET_POOL`.  The `SET_POOL` action is only available in version 2 or later of the file format. |
|adjustment     | An integer between zero (0) and one million (1000000). |
|pool           | The name of a previously defined thread pool.  The literal string `default` refers to the default thread pool.  The `pool` property name is only available in version 2 or later of the file format. |
|flags          | One or more of `NONE`, `DISABLE`, `PRINT`, `STOP`, and `DYN_POOL` see [flags syntax](#flags-syntax).  The `DYN_POOL` flag is only available in version 2 or later of the file format. |
|mode           | One or more of `NONE`, `EXACT`, `GLOB`, `REGEXP`, and `NOCASE`, see [flags syntax](#flags-syntax). |
|originHost     | Any pattern string suitable for match mode.  May not contain whitespace. |
|originTask     | Any pattern string suitable for match mode.  May not contain whitespace. |
|user           | Any pattern string suitable for match mode.  May not contain whitespace. |
|sql            | Any pattern string suitable for match mode.  May contain whitespace. |
|fingerprint    | SQLite compatible BLOB, with a size of exactly sixteen (16) bytes, as string literal, e.g. `x'0123456789abcdef0123456789abcdef'`. |

### SQL query fingerprints

In order to successfully make use of the `fingerprint` property in rule
definitions, the `strict_double_quotes` tunable must be enabled.  This is
necessary to ensure internal consistency with the fingerprints calculated
by threads that do not have access to the SQL query preparation subsystem.

### Flags syntax

For property values that represent a set of flags, e.g. for the `flags` and
`mode` properties, multiple choices from the set of possible values may be
specified, delimited by whitespace or commas.  The semicolon may be used to
delimit the end of these property values.  A set of flags may begin with an
opening curly brace `{` and end with a closing curly brace `}`, to enhance
its readability.

### Rule actions and adjustments

If all criteria specified for a rule are matched an action is taken.  If the
action is `NONE`, nothing is done.  This can be useful if the server has been
configured to emit a log message when a ruleset is matched, e.g. this allows
rulesets to effectively monitor activity without otherwise impacting it.  If
the action is `REJECT` or `REJECT_ALL`, the SQL query will be marked as
rejected.  The difference between these two actions is that a `REJECT_ALL`
action will prevent a SQL query from being retried on another node whereas an
action of `REJECT` will not.  These marks of rejection can be removed by
matching a subsequent rule with an action of `UNREJECT`.  If the SQL query is
marked as rejected and the `STOP` rule flag is specified for the rule (**or**
there are no further rules to process), the SQL query will be rejected and an
error will be emitted to the client.  If the action is `LOW_PRIO`, the relative
priority of the SQL query will be decreased by the amount specified by the
associated `adjustment` property value.  If the action is `HIGH_PRIO`, the
relative priority of the SQL query will be increased by the amount specified by
the associated `adjustment` property value.  If the action is `LOW_PRIO`,
`HIGH_PRIO`, or `SET_POOL`, the target thread pool for the SQL query will be
changed to the one associated with the final rule that specified an action of
`SET_POOL`.  The target thread pool can always be changed by a subsequently
matched rule with an action of `SET_POOL` and only the final target thread pool
for a given SQL query will be honored.  When processing the `SET_POOL` action,
the final resulting relative priority of the SQL query value will be honored,
i.e. even though the final resulting action is not `LOW_PRIO` or `HIGH_PRIO`.

### Rule flags

The `NONE` rule flag has no effect.  If all criteria specified for a rule are
matched and the `STOP` rule flag is present, further rule matching is skipped
and the current ruleset result is returned.  If all criteria specified for a
rule are matched and the `PRINT` rule flag is present, a detailed diagnostic
message will be emitted into the log file.  Rules with the `DISABLE` flag set
will not be evaluated.  If the `DYN_POOL` flag is set, the final target thread
pool, if any, will be created if it does not already exist.

### Match modes

The match mode `NONE` performs no matching.  If a match mode of `NONE` is
specified and any rule criteria are present, the results are undefined.  If
no criteria are present for a rule then rule is always treated as a match.
The match mode `EXACT`, as the name suggests, treats the property value as
a string literal to be matched exactly.  The match mode `GLOB` treats the
property value as a pattern compatible with the [SQLite GLOB operator syntax](https://www.sqlite.org/lang_expr.html#glob).
The match mode `REGEXP` treats the property value as a [regular expression](https://en.wikipedia.org/wiki/Regular_expression)
compatible with the [SQLite regular expression extension](https://www.sqlite.org/src/artifact?ci=trunk&filename=ext/misc/regexp.c).
The match mode `NOCASE` may be combined with another match mode to enable
case-insensitive matching.

### Annotated Example #1

```
#######################################################
# The 'version' line is required and must be the first
# line that is not blank and not a comment.  Currently,
# the only valid versions are '1' and '2'.  When using
# thread pool definitions, the version must be '2'.
#######################################################

version 2

#######################################################
# Each thread pool definition should occupy one line.
# The name of the thread pool is specified immediately
# after the 'pool' keyword.  The 'threads' keyword is
# optional and is used to specify the maximum number
# of threads in the thread pool.
#######################################################

pool extra1 threads 3

#######################################################
# Each rule definition may occupy multiple lines, with
# each line containing one or more property name/value
# pairs, delimited by whitespace.  For property values
# that may contain whitepace, e.g. 'sql', semicolon is
# used to indicate the end of that property value.
#######################################################

# The first rule is designed to reject all SQL queries
# matching the specified fingerprint, which corresponds
# to the SQL query 'SELECT * FROM t1'.  There are no
# other criteria for the rule.

rule 1 action REJECT
rule 1 fingerprint X'a9c8b6ddb5b9e55ee41b7f5a46ec4e45'
rule 1 flags STOP

# The second rule is designed to lower the priority of
# SQL queries that originate from a database user with
# the name 'Robert'.  The SQL query will be allowed to
# run; however, other higher priority SQL queries may
# run first even if they were submitted later.

rule 2 action LOW_PRIO
rule 2 adjustment 1000
rule 2 user Robert

# The third rule is designed to raise the priority of
# SQL queries that originate from tasks with a name
# matching the specified GLOB pattern.  In this case,
# it refers to the command line SQL query tool.

rule 3 action HIGH_PRIO
rule 3 adjustment 1000
rule 3 mode GLOB
rule 3 originTask */cdb2sql

# The fourth rule is designed to emit a trace when a
# SQL query matching the specified regular expression
# is seen.  In theory, subsequent rules could alter
# the priority of this SQL query or prevent if from
# executing.

rule 4 action NONE
rule 4 flags PRINT
rule 4 mode REGEXP NOCASE
rule 4 sql ^CREATE +.*$

# The fifth rule is designed to allow SQL queries
# that may have been previously marked as rejected to
# run if they originate from a database user with the
# name 'david'.  For this rule, the user name will be
# compared in a case-insensitive manner.

rule 5 action UNREJECT
rule 5 mode EXACT NOCASE
rule 5 user david

# The six and seventh rules are designed to allow some
# specific SQL queries to be serviced by a non-default
# thread pool (e.g. they cannot wait for the default
# thread pool, etc).

rule 6 action SET_POOL
rule 6 pool extra1
rule 6 flags PRINT STOP
rule 6 mode REGEXP NOCASE
rule 6 sql sleep

rule 7 action SET_POOL
rule 7 flags PRINT STOP DYN_POOL
rule 7 pool extra2
rule 7 mode REGEXP NOCASE
rule 7 sql comdb2_host
```
