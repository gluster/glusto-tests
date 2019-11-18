# Contributor Guide
This provides the guidelines for contributing to the glusto-tests project.

For detailed guidelines on development of tests, visit the [developer-guide](https://github.com/gluster/glusto-tests/blob/master/docs/userguide/developer-guide.rst).

## 1. Initial Preparation
The glusto-tests development workflow revolves around [Git](https://git-scm.com/), [Gerrit](https://review.gluster.org/) and [Jenkins](https://build.gluster.org/).

Using these tools requires some initial preparation.

### 1.1 Dev System Setup
You should install and setup Git on your development system. Use your distribution specific package manger to install git. After installation configure git. At the minimum, set a git user email. To set the email do,

```
$ git config --global user.name "Name"
$ git config --global user.email <email address>
```
You should also generate an ssh key pair if you haven't already done it. To generate a key pair do,

```
$ ssh-keygen
```
and follow the instructions.

### 1.2 Gerrit Setup
To contribute to glusto-tests, you should first register on [gerrit](https://review.gluster.org).

After registration, you will need to select a username, set a preferred email and upload the ssh public key in gerrit. You can do this from the gerrit settings page. Make sure that you set the preferred email to the email you configured for git.

### 1.3 Get the glusto-tests project
Git clone the glusto-tests project using

```
<ssh://><username>@review.gluster.org/glusto-tests
```

(replace with your gerrit username).

```
$ git clone ssh://<username>@review.gluster.org/glusto-tests
```

This will clone the glusto-tests project into a subdirectory named glusto-tests with the master branch checked out.

It is essential that you use this link to clone, or else you will not be able to submit patches to gerrit for review.

Note: 'ssh' is used to clone so as to avoid repetitive steps of entering credentials every time you submit a patch for review. If you wish, you may use https or git to clone this project.

## 2. Actual Development
The commands in this section are to be run inside the glusto-tests project directory (where it was cloned).

### 2.1 Create a Development Branch
It is recommended to use separate local development branches for each change you want to contribute to glusto-tests. To create a development branch, first checkout the upstream branch you want to work on and update it.

```
$ git checkout master
$ git pull
```

Now, create a new branch from master and switch to the new branch. It is recommended to have descriptive branch names.

```
$ git branch <descriptive-branch-name>
$ git checkout <descriptive-branch-name>
```

OR,

```
$ git checkout -b <descriptive-branch-name>
```

to do both in one command.

### 2.2 Make Changes
Once you've switched to the development branch, you can perform the actual code changes from the development branch. Verify your changes by testing them in your local machine.

For more information, visit [Writing tests in glusto-tests](https://github.com/gluster/glusto-tests#writing-tests-in-glusto-tests)

### 2.3 Testing Changes
It is always a good measure to test the code change before submitting it as a patch.
The following guidelines must be followed while development:

* Test case changes:
    * If the code change is as a new test case, it should be tested with all the valid volume types and mount types that it can be tested on.
    * If the code change is on existing test cases then it needs to be tested with the volume and mount type that it can be (unless it is a change in the volume type/mount type).
* Library changes
    * If the code change is as a new library, the library functions must be tested.
    * If the code change is on existing libraries, the dependent test cases must be re-tested in order to assure there is no break in the test case itself.
* Other changes
    * If the code change is on README / documentation / guides , it is good measure to validate that proper grammar is utilized.

For detailed information on running the glusto-tests, visit [How to run the test case](https://github.com/gluster/glusto-tests#how-to-run-the-test-case)

### 2.4 Commit Your Changes
If you haven't broken anything, you can now commit your changes. First identify the files that you modified/added/deleted using git-status and stage these files.

```
$ git status
$ git add <list of modified files>
```

Now, commit these changes using

```
$ git commit -s
```

It is essential that you commit with the '-s' option, which will sign-off the commit with your configured email, as gerrit is configured to reject patches which are not signed-off.

**Commit message:**

Provide a meaningful commit message.

Commit message guidelines:

```
Short (50 chars or less) summary of changes

More detailed explanatory text. Wrap it to 72 characters. The blank
line separating the summary from the body is critical (unless you omit
the body entirely).

Write your commit message in the imperative: "Fix bug" and not "Fixed
bug" or "Fixes bug". This convention matches up with commit messages
generated by commands like git merge and git revert.

Further paragraphs come after blank lines.

- Bullet points are okay, too.
- Typically a hyphen or asterisk is used for the bullet, followed by a
  single space. Use a hanging indent.
```

### 2.5 Submit for Review
To make sure that you don't stumble upon any irregularity while submitting your changes for review, the origin must be renamed to 'gerrit'.
Perform the following command from the root of glusto-tests:

```
$ git remote rename origin gerrit
```

The gerrit review process used for glusto-tests is by 'git-review'. You must have git-review installed in order to submit a patch for review.

Install git-review using pip:

```
$ pip install git-review
```

Once installed, you simply need to provide the review command:

```
$ git-review
```

After a successful git-review, you will get a url in CLI which is the gerrit link to your patch. This patch can now be reviewed.

### 2.6 Review Process
Your change will now be reviewed by the glusto-tests maintainers and component owners on gerrit. You can follow and take part in the review process on the change at the review url. The review process involves several steps.

### 2.7 Automated Verification
Every change submitted to gerrit triggers an initial automated verification on jenkins. The automated verification ensures that your change adheres to the [PEP008 style-guide](https://www.python.org/dev/peps/pep-0008/) and is free of any [pylint](https://docs.pylint.org/en/1.6.0/tutorial.html) or [pyflakes](http://flake8.pycqa.org/en/latest/index.html) errors.

### 2.8 Formal Review
Once the auto verification is successful, the component owners will perform a formal review. If they are okay with your change, they will give a positive review. If not they will give a negative review and add comments on the reasons.

If your change gets a negative review, you will need to address the comments and resubmit your change.

### 2.9 Resubmission

Switch to your development branch and make new changes to address the review comments. Test to see if the new changes are working.

Stage your changes and commit your new changes using,

```
$ git commit --amend
```

'--amend' is required to ensure that you update your original commit and not create a new commit.

Now you can resubmit the updated commit for review using:

```
$ git-review
```

The formal review process could take a long time. To increase chances for a speedy review, you can add the component owners as reviewers on the gerrit review page. This will ensure they notice the change.

### 2.10 Verification
After a component owner has given a positive review, a maintainer will run the regression test suite on your change to verify that your change works and hasn't broken anything. This verification is done with the help of Jenkins.

If the verification fails, you will need to make necessary changes and resubmit an updated commit for review.

### 2.11 Acceptance
After successful verification, a maintainer will merge your change into the upstream glusto-tests master branch. Your change will now be available in the upstream git repo for everyone to use.

## 3. Contact Information

You can get in touch with the community by joining [automated-testing](https://lists.gluster.org/mailman/listinfo/automated-testing). You may subscribe to the mailing list for any updates or simply send an email for any queries.

Other gluster related mailing lists are available at [Mailman](https://lists.gluster.org/mailman/listinfo).