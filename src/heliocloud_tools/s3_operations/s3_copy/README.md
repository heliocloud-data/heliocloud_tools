# S3 Bucket Copy scripts

## About

This code will help copy objects over preserving their checksums/metadata as desired.

## Usage
Note down source and destination bucket names + paths, and use a list of files to copy from the source to destination.


For example:

Given these mock lines from `copyList.csv`:
```
sdac/iris/iris_data/level2_compressed/2014/02/14/20140214_224401_3860259280/data_SDO.tar.gz
sdac/iris/iris_data/level2_compressed/2014/02/13/20140213_221421_3860259280/data_SDO2.tar.gz
sdac/iris/iris_data/level2_compressed/2014/02/13/20140213_221421_3860259280/data_SDO3.tar.gz
```

We run the script the following way:
```py
python make-cp-script.py
    -i copyList.csv             *** Input file name
    -src scratch-data-ops       *** Source bucket/path
    -dst gov-nasa-hdrl-data1    *** Destination bucket/path
    -s 120                      *** Sleep duration in seconds
    -n 10                       *** Number of concurrent copies
    -c 1                        *** Checksum mode? (only uses CRC64NVME)
    > copy-iris-to-tops.sh      *** Your output file name.
```
This the first line generated:
```
aws s3 cp 
    s3://scratch-data-ops/sdac/iris/data_SDO.tar.gz
    s3://gov-nasa-hdrl-data1/sdac/iris/data_SDO.tar.gz
    --checksum-algorithm CRC64NVME
    >> ./copy_log.txt &
```
Another more useful example:
Here we are copying the **CONTENTS** of `/sdac/iris/iris_data` and placing them in just `/iris_test/`

**THIS REQUIRES THAT YOU RUN SED to remove that part of the path from the source file, giving you:**
```
level2_compressed/2014/02/14/20140214_224401_3860259280/data_SDO.tar.gz
level2_compressed/2014/02/14/20140214_224401_3860259280/data_SDO2.tar.gz
level2_compressed/2014/02/14/20140214_224401_3860259280/data_SDO3.tar.gz
```

This preserves the directory tree WITHIN iris_data but does not force you to have /sdac/iris/iris_data/

you only get iris_data/* placed into gov-nasa-hdrl-data1/iris_test
The command to generate the copy script then becomes:
```
python make-cp-script.py
    -i copyList.csv
    -src scratch-data-ops/sdac/iris/iris_data
    -dst gov-nasa-hdrl-data1/iris_test
    -s 120
    -n 10
    -c 1
    > run-me.sh
```
This will be the first line generated instead:
```
aws s3 cp
    s3://scratch-data-ops/sdac/iris/iris_data/data_SDO.tar.gz
    s3://gov-nasa-hdrl-data1/iris_test/data_SDO.tar.gz        ← notice how there is no sdac/iris/iris_data, just the contents
    --checksum-algorithm CRC64NVME
    >> ./copy_log.txt &
```

