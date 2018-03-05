## dbt models to test parent details macro(s)

### Instructions
* Run `dbt seed`
* Change references in `models/example/*` from `analytics_claire` to your target schema (it's late and I couldn't get this to work)
* `dbt run`

### What is this?
* Based on [this discussion](https://getdbt.slack.com/archives/C0VLZPLAE/p1519866500000202), I had a go at writing some macros for parent/child relationships
* I've written [one](parent_details.sql) that basically just loops to an arbitrary level, I'd love feedback on this - what's good, what can be improved, be ruthless! Even if it's just linting (I may have gotten lazy with it)
* I've also been thinking about how you would do this dynamically, and put my work [here](parent_details_advanced.sql). I started out by writing SQL that I could execute, and then changing it into pieces that got compiled into the right shape, but it doesn't actually work as a macro. I've left lots of notes in comments, if you have any suggestions I'd be happy to hear them (even if the suggestion is just to keep the simpler version)