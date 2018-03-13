## dbt models to test parent details macro(s)

### Instructions
```
$ dbt seed
$ dbt run
```
* Comment out/in the different macros in [family_tree_modelled](models/example/family_tree_modelled.sql) to see the macros in action

### What is this?
* Based on [this discussion](https://getdbt.slack.com/archives/C0VLZPLAE/p1519866500000202), I had a go at writing some macros for parent/child relationships
* I had a number of different ideas about how to implement this, so ended up writing them all (good practice for me), see [parent_details_macros](macros/parent_details_macros.sql)
* I also split out some of the more generalisable macros into separate files

### Notes:
* Requires dbt v0.10.0 or greater