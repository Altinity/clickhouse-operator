# Submitting your patch

Thanks for taking the time to contribute to `clickhouse-operator`!

## Intro
`clickhouse-operator` contribution process is built around standard git _Pull Requests_. 


## How to make PR

Please, **do not** make PR into `master` branch. 
We intend to keep `master` clean and stable and will not accept commits directly into `master`.
We always have dedicated branch, named as `x.y.z` (for example, `0.9.1` at the time of this writing), which is used as a  testbed for next release.
Please, make your PR into this branch. If you are not sure which branch to use, create new issue to discuss, and we'd be happy to assist.

Your submission should not contain more than one commit. Please **squash your commits**.
In case you'd like to introduce several features, make several PRs, please. 

## Sign Your Work

Every PR has to be signed. The sign-off is a text line at the end of the commit's text description.
Your signature certifies that you wrote the patch or otherwise have the right to contribute it to `clickhouse-operator`.

Developer Certificate of Origin is available at [developercertificate.org](https://developercertificate.org/):

```text
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

Please add the following line to every git commit description (with your real name, instead of John Doe):

    Signed-off-by: John Doe <john.doe@example.com>

Please, use your real name for sign-off.

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`.

Your `git log` information for your commit should look something like this:

```
Author: John Doe <john.doe@example.com>
Date:   Mon Jan 24 12:34:56 2020 +0200

    Update README

    Signed-off-by: John Doe <john.doe@example.com>
```

Notice the `Author` and `Signed-off-by` lines **must match**.
