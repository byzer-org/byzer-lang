
## How to contribute to MLSQL

We use a review-then-commit workflow in MLSQL for all contributions.

* Engage  -> Code -> Review -> Commit


## Engage

We use [Github Issue](https://github.com/allwefantasy/streamingpro/issues) as an issue tracking and project management tool, as well as a way to communicate among a very diverse and distributed set of contributors.

## Code

We use GitHub’s pull request functionality to review proposed code changes. If you do not already have a personal GitHub account, sign up [here](https://github.com/).

### Git config

Ensure to finish the below config(user.email, user.name) before starting PR works.

```
$ git config --global user.email "you@example.com"
$ git config --global user.name "Your Name"
```

### Fork the repository on GitHub

Go to the [MLSQL](https://github.com/allwefantasy/streamingpro) and fork the repository to your account. This will be your private workspace for staging changes.


### Clone the repository locally

You are now ready to create the development environment on your local machine.

```
$ git clone https://github.com/allwefantasy/streamingpro.git
$ cd streamingpro
```


You are now ready to start developing!

### Create a branch in your fork

You’ll work on your contribution in a branch in your own (forked) repository.
Create a local branch, initialized with the state of the branch you expect your changes to be merged into.
Keep in mind that we use several branches, including master, feature-specific, and release-specific branches.
If you are unsure, initialize with the state of the master branch.



```
$ git fetch --all
$ git checkout -b <my-branch> origin/master
```

At this point, you can start making and committing changes to this branch in a standard way.

### Syncing and pushing your branch

Periodically while you work, and certainly before submitting a pull request,
you should update your branch with the most recent changes to the target branch.

```
 $ git pull --rebase
```

Remember to always use --rebase parameter to avoid extraneous merge commits.

To push your local, committed changes to your (forked) repository on GitHub, run:

```
$ git push <GitHub_user> <my-branch>
```


### Review

Once the initial code is complete,
it’s time to start the code review process. We review and discuss all code,
no matter who authors it. It’s a great way to build community, since you can learn from other developers,
and they become familiar with your contribution.
It also builds a strong project by encouraging a high quality bar and keeping code consistent throughout the project.

### Create a pull request

Organize your commits to make your reviewer’s job easier. Use the following command to re-order, squash, edit, or change description of individual commits.

```
$ git rebase -i origin/master
```

Navigate to the [MLSQL Github](https://github.com/allwefantasy/streamingpro) to create a pull request. The title of the pull request should be strictly in the following format:


```
title:

[DEV][add] Docker runtime support #IssueNumber

description:

1. What changes were proposed in this pull request?
2. How was this patch tested?
3. Spark Core Compatibility

```



