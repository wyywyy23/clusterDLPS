[![Build Status][travis-badge]][travis-link]
[![GitHub Release][release-badge]][release-link]
[![License: LGPL v3][license-badge]](LICENSE)
[![CodeFactor][codefactor-badge]][codefactor-link]

# WRENCH-WorkQueue Implementation

<img src="https://raw.githubusercontent.com/wrench-project/wrench/master/doc/images/logo-vertical.png" width="100" />

This project is a [WRENCH](http://wrench-project.org)-based simulator 
of the [WorkQueue](https://ccl.cse.nd.edu/software/workqueue/) framework.

**WRENCH Research Paper:**
- H. Casanova, R. Ferreira da Silva, R. Tanaka, S. Pandey, G. Jethwani, W. Koch, S.
  Albrecht, J. Oeth, and F. Suter, [Developing Accurate and Scalable Simulators of
  Production Workflow Management Systems with 
  WRENCH](https://rafaelsilva.com/files/publications/casanova2020fgcs.pdf), 
  Future Generation Computer Systems, 2020. 

## Prerequisites

WRENCH-WorkQueue is fully developed in C++. The code follows the C++11 standard, 
and thus older compilers tend to fail the compilation process. Therefore, we strongly 
recommend users to satisfy the following requirements:

- **CMake** - version 3.2.3 or higher
  
And, one of the following:
- **g++** - version 5.0 or higher
- **clang** - version 3.6 or higher


## Dependencies

- [WRENCH](http://wrench-project.org/) - [version 1.6](https://github.com/wrench-project/wrench)


## Building From Source

If all dependencies are installed, compiling and installing WRENCH-WorkQueue is as simple as running:

```bash
cmake .
make
make install  # try "sudo make install" if you do not have the permission to write
```


## Get in Touch

The main channel to reach the WRENCH-WorkQueue team is via the support email: 
[support@wrench-project.org](mailto:support@wrench-project.org).

**Bug Report / Feature Request:** our preferred channel to report a bug or request 
a feature is via WRENCH-WorkQueue's 
[Github Issues Track](https://github.com/wrench-project/workqueue/issues).


[travis-badge]:          https://travis-ci.org/wrench-project/workqueue.svg?branch=master
[travis-link]:           https://travis-ci.org/wrench-project/workqueue
[license-badge]:         https://img.shields.io/badge/License-LGPL%20v3-blue.svg
[release-badge]:         https://img.shields.io/github/release/wrench-project/workqueue/all.svg
[release-link]:          https://github.com/wrench-project/workqueue/releases
[codefactor-badge]:      https://www.codefactor.io/repository/github/wrench-project/workqueue/badge
[codefactor-link]:       https://www.codefactor.io/repository/github/wrench-project/workqueue
