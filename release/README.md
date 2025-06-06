<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Release Guide

## Start a tracking issue

Create a tracking issue for the release with title `Tracking issue for release <release version>`. Add the issue to the
corresponding milestone for the release, and edit the issue description as below.

```markdown
## Tasks

### Issues

- [ ] All other issues in the [release milestone](https://github.com/apache/hudi-rs/milestone/1) should be closed.

> [!IMPORTANT]
> Highlight blocker issues if any

- [ ] https://github.com/apache/hudi-rs/issues/xxx
- [ ] https://github.com/apache/hudi-rs/issues/xxx

### Prepare release candidate(s)

- [ ] Create a release branch 
- [ ] Bump the version in the release branch for the target RC
- [ ] Bump the version in `main` branch
- [ ] Upload the target RC artifacts to the ASF dev repo (SVN)
- [ ] Verify the target RC artifacts
- [ ] Push a desired RC release git tag to the release branch
- [ ] Start VOTE in `dev@hudi.apache.org`

> [!IMPORTANT]
> Proceed from here only after VOTE passes.

### Official release

- [ ] Bump the version in the release branch for the official release
- [ ] Push the official release git tag to the release branch
- [ ] Upload the release artifacts to the ASF release repo (SVN)
- [ ] Merge a PR to update the changelog in `main` branch
- [ ] Publish release notes in https://github.com/apache/hudi-rs/releases
- [ ] Register ASF release in https://reporter.apache.org/addrelease.html?hudi
- [ ] Send `ANNOUNCE` email to dev and user email lists
- [ ] Close this tracking issue and the release milestone
```

## ASF work

### Upload source release to SVN dev

> [!NOTE]
> Make sure you're using a computer that has your code-signing gpg key installed.

Run the below script to create and upload the source release artifacts.

```shell
RELEASE_VER=0.1.0-rc.1

./release/upload_src_release_to_dev.sh $RELEASE_VER ${YOUR CODE-SIGNING KEY ID}
```

Run the below script to verify the source release.

```shell
RELEASE_VER=0.1.0-rc.1

./release/verify_src_release.sh $RELEASE_VER dev
```

Fix any issues if found before proceeding to the next step.

## GitHub work

> [!NOTE]
> We adhere to [Semantic Versioning](https://semver.org/), and create a release branch for each major or minor release.

### Bump version in main branch

Execute the below script that creates a branch with the new version changes committed. Submit a PR using that.

```shell
./release/bump_version_in_main.sh
```

### Bump version in release branch

> [!NOTE]
> When working on a release branch, use a local clone of `apache/hudi-rs` instead of your own fork.

For a major or minor release, create a release branch in the format of `release/[0-9]+.[0-9]+.x` matching the target
release version. For example, if it's `0.2.0`, cut a branch named `release/0.2.x`, if it's `1.0.0`, cut a branch
named `release/1.0.x`.

For a patch release, don't cut a new branch, use its base release branch. For example, if it's `0.3.1`, cherry-pick
fixes to `release/0.3.x`.

Create and merge a PR to bump the major or minor version on the `main` branch. For example, if the current version
is `0.3.0`, bump it to `0.4.0` or `1.0.0`.

On the release branch, bump the version to indicate pre-release by pushing a commit.

- If it is meant for internal testing, go with `alpha.1`, `alpha.2`, etc
- If it is meant for public testing, go with `beta.1`, `beta.2`, etc
- If it is ready for voting, go with `rc.1`, `rc.2`, etc

```shell
RELEASE_VER=0.2.0-rc.1

cargo set-version $RELEASE_VER --manifest-path crates/hudi/Cargo.toml
git commit -am "build(release): bump version to $RELEASE_VER"
```

### Testing

Once the "bump version" commit is pushed, CI will be triggered and all tests need to pass before proceed to the next.

Perform any release related tests, such as integration tests, before proceeding to the next step.

### Generate changelog

We use [cliff](https://git-cliff.org/) to generate changelogs. Install it by running the below command.

```shell
cargo install git-cliff
```

Switch to the release branch (org: apache/hudi-rs), generate a changelog for the release by running the below script.

```shell
# specify the previous release version

# for mac
git cliff release-{previous_release_version}..HEAD | pbcopy

# for linux
git cliff release-{previous_release_version}..HEAD | xclip
```

Paste the changelog output as a comment on the tracking issue.

### Push tag

> [!IMPORTANT]
> The version in the code should be the target release version.

Push a tag to the commit that matches to the version in the form of `release-*`. For example,

- If the release is `0.1.0-rc.1`, the tag should `release-0.1.0-rc.1`
- If the release is `2.0.0-beta.2`, the tag should `release-2.0.0-beta.2`

> [!CAUTION]
> Pushing a matching tag to the upstream (apache) branch will trigger CI to publish the artifacts to crates.io and
> pypi.org, which, if successful, is irreversible. Same versions are not allowed to publish more than once.

```shell
RELEASE_VER=0.1.0-rc.1

git tag release-$RELEASE_VER
git push origin release-$RELEASE_VER
```

Once the CI completes, check crates.io and pypi.org for the new release artifacts.

### Start a `[VOTE]` thread

```text
[VOTE] hudi-rs 0.1.0, release candidate #2

Hi everyone,

Please review and vote on hudi-rs 0.1.0-rc.2 as follows:

[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

The complete staging area is available for you to review:

* Release tracking issue is up-to-date [1]
* Categorized changelog for this release [2]
* Source release has been deployed to dist.apache.org [3]
* Source release can be verified using this script [4]
* Source code commit is tagged as "release-0.1.0-rc.2" [5]
* Source code commit CI has passed [6]
* Python artifacts have been published to pypi.org [7]
* Rust artifacts have been published to crates.io [8]

The vote will be open for at least 72 hours. It is adopted by majority
approval, with at least 3 PMC affirmative votes.

Thanks,
Release Manager

[1] https://github.com/apache/hudi-rs/issues/62
[2] https://github.com/apache/hudi-rs/issues/62#issuecomment-2224322166
[3] https://dist.apache.org/repos/dist/dev/hudi/hudi-rs-0.1.0-rc.2/
[4] https://github.com/apache/hudi-rs/blob/7b2d199c180bf36e2fac5e03559fffbfe00bf5fe/release/verify_src_release.sh
[5] https://github.com/apache/hudi-rs/releases/tag/release-0.1.0-rc.2
[6] https://github.com/apache/hudi-rs/actions/runs/9901188924
[7] https://pypi.org/project/hudi/0.1.0rc2/
[8] https://crates.io/crates/hudi/0.1.0-rc.2
```

## After VOTE passes

### Bump version in the release branch

Remove the pre-release suffix from the version in the release branch.

```shell
RELEASE_VER=0.2.0

cargo set-version $RELEASE_VER --manifest-path crates/hudi/Cargo.toml
git commit -am "build(release): bump version to $RELEASE_VER"
```

Wait for the CI to pass before proceeding to the next step.

### Upload source release to SVN release

Run the below script to create and upload the source release artifacts.

```shell
RELEASE_VER=0.2.0

./release/publish_src_release.sh $RELEASE_VER ${YOUR CODE-SIGNING KEY ID}
```

Run the below script to verify the source release.

```shell
RELEASE_VER=0.2.0

./release/verify_src_release.sh $RELEASE_VER release
```

There shouldn't be any issue in verifying the source release at this step. But if any error came out from it, 
revert the last version bump commit, fix the reported error, and start a new release candidate.

### Push the release tag

> [!CAUTION]
> Pushing a matching tag to the upstream (apache) branch will trigger CI to publish the artifacts to crates.io and
> pypi.org, which, if successful, is irreversible. Same versions are not allowed to publish more than once.

```shell
RELEASE_VER=0.2.0

git tag release-$RELEASE_VER
git push origin release-$RELEASE_VER
```

Once the CI completes, check crates.io and pypi.org for the new release artifacts.

### Update the change log

Use `git cliff` to prepend the current release's change to `changelog.md` in the main branch.

Close the tracking issue.

### Send `ANNOUNCE` email

```text
subject: [ANNOUNCE] hudi-rs ${RELEASE_VER} released

Hi all,

The Apache Hudi community is pleased to announce the release ${RELEASE_VER} of
hudi-rs <https://github.com/apache/hudi-rs>, the native Rust implementation for
Apache Hudi, with Python API bindings.

Highlights for this release:

<insert highlights here based on the changelog>

The release notes can be found here
https://github.com/apache/hudi-rs/releases/tag/release-${RELEASE_VER}

The source releases are available here
https://dist.apache.org/repos/dist/release/hudi/hudi-rs-${RELEASE_VER}/

Please refer to the readme for installation and usage examples
https://github.com/apache/hudi-rs/blob/main/README.md

The Hudi community is active on these channels - we welcome you to engage 
with us!

- LinkedIn <https://www.linkedin.com/company/apache-hudi/>
- X/Twitter <ht...@apachehudi>
- YouTube <ht...@apachehudi>
- Slack support
<https://join.slack.com/t/apache-hudi/shared_invite/zt-2ggm1fub8-_yt4Reu9djwqqVRFC7X49g>

For users in China, follow WeChat "ApacheHudi" 微信公众号 for news and blogs,
and join DingTalk group 钉钉群 35087066 for questions.

Cheers,

<name>, Release manager
```
