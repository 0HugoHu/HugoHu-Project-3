# IDS706-Python-Template
Duke IDS706 course project template

Yadong (Hugo) Hu

```
├── Dockerfile               # The file to build a container using docker
├── CONTRIBUTING.md          # Onboarding instructions for new contributors
├── .github                  # Github metadata for repository
│   ├── release_message.sh   # A script to generate a release message
│   └── workflows            # The CI pipeline for GitHub Actions
├── .gitignore               # A list of files to ignore when pushing to GitHub
├── HISTORY.md               # Auto-generated list of changes to the project
├── LICENSE                  # The license for the project
├── Makefile                 # A collection of utilities to manage the project
├── MANIFEST.in              # A list of files to include in a package
├── ids706_python_template   # The main Python package for any future project
│   ├── base.py              # The base module for the project
│   ├── __init__.py          # This tells Python that this is a package
│   ├── __main__.py          # The entry point for the project
│   └── VERSION              # The version for the project is kept in a static file
├── README.md                # The main readme for the project
├── setup.py                 # The setup.py file for installing and packaging the project
├── requirements.txt         # An empty file to hold the requirements for the project
├── requirements-test.txt    # List of requirements for testing and development
├── setup.py                 # The setup.py file for installing and packaging the project
└── tests                    # Unit tests for the project
    ├── conftest.py          # Configuration, hooks, and fixtures for pytest
    ├── __init__.py          # This tells Python that this is a test package
    └── test_base.py         # The base test case for the project
└── .tutorial                # Screenshots of how to run docker
```

<br />

To build and run the project, go to the ```Dockerfile```, and make sure your **Docker** is running on your computer.

You can hit the run button on the first line if you are using the PyCharm IDE:

![](/.tutorial/step1.png)

Or you need to configure your Dockerfile configuration (please use all default values, connect to Docker Daemon on Mac with unix:///var/run/docker.sock):

![](/.tutorial/step2.png)

Then, hit the run button, and the Docker image begins to build:

![](/.tutorial/step3.png)

The output should be a string:

![](/.tutorial/step4.png)

<br />
