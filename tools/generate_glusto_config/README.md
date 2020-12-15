# generate_glusto_config
Tool to generate config file for executing glusto tests.

## Prerequisites
Python 3.x

## Installation
1. Change directory to the project directory.

```
# cd tools/generate_glusto_config
```

2. Now run the installation script.

```
# python3 setup.py install
```

3. To check run:

```
# generate_glusto_config --help
```

## Usage
Pass arguments to the script as shown below:

```
# generate_glusto_config -c examples/sample_glusto_config.yaml -t glusto_config_template.jinja -o output_config.yml
```

## Licence
[GPLv3](https://github.com/gluster/glusto-tests/blob/master/LICENSE)
