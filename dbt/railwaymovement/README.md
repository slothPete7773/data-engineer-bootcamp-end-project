Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test

### Locating profiles.yml file

Considering that this dbt project is integrate within Astronomer Cosmos library. 
It needed to explicitly locate the `profiles.yml` outside of the dbt project library. Otherwise, there will be conflict 
in the connections configured in the Airflow Connection. Astronomer Cosmos will unable to find the profile connection configured in the Airflow Connection

```
. # dbt/
├── logs/
├── railwaymovement/ 
│   ├── analyses/
│   ├── logs/
│   ├── models/
│   ├── tests/
│   ...
│   │
│   └── dbt_project.yml
└── profiles.yml
...
```

Therefore, when execute the dbt commands, add the profile directory option to locate the user-defined function. 
Only locate the container directory, don't need to locate the exact file location.

Therefore, always remember to do 2 things when executing dbt with Astronomer Cosmos.

1. Execute the dbt command when reside in the dbt project directory.
2. Provide the correct `profiles.yml` directory path for `--profiles-dir` option.

The previous is not strictly required, just a convenience things to do.
```sh
$ dbt debug --profiles-dir ..
```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
