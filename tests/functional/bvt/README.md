# BVT
## What is Build Verification or Build Validation Test?
In software testing, a Build Verification Test (BVT), also known as
Build Acceptance Test, is a set of tests run on each new build of a product
to verify that the build is testable before the build is released into the
hands of the test team.

## Scope of Testing:
The build acceptance test is generally a short set of tests,
which exercises the mainstream functionality of the application software.
Any build that fails the build verification test is rejected,
and testing continues on the previous build (provided there has been at
least one build that has passed the acceptance test)

Source of definition: https://en.wikipedia.org/wiki/Build_verification_test

## Gluster BVT

BVT is divided in 2 set of tests.

### BVT-Basic

BVT-Basic is the first set of tests to qualify the build.
Tests include validating services (glusterd, smb etc),
validating volume operations (create, start, stop , status)

### BVT-VVT ( VVT: Volume verification test)

BVT-VVT is the second set of tests to qualify whether the build is good to
be consumed for further testing once BVT-Basic passes.
BVT-VVT covers the following gluster specific test on all combinations
of volume and mount types supported by gluster.

Test Case Summary:
1) Creates a volume
2) Mounts the volume
3) Run IO from the mount.
4) teardown the mount
5) teardown the volume

Volume types:
- Distribute
- Replicate
- Distribute-Replicate
- Disperse
- Distribute-Disperse

Mount types:
- Glusterfs
- Nfs
- CIFS
