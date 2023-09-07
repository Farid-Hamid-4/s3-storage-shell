# Assignment 1: AWS S3 Storage Shell 

Name: Farid Hamid
Course: CIS 4010 - Cloud Computing
Student ID: 1067867

## REGION
`ca-central-1`

## IMPORTANT 
Must have python >= 3.4 to use Pathlib library
`helpers.py` contains helper functions. Ensure it is in the same directory as `s5.py`

## How to run shell
1. Ensure that the `S5-S3.conf` file is in the same directory as the `s5.py` file
2. Run the command: `python3 s5.py`
3. To exit or quit the program, simply type `exit` or `quit`

## Normal Behavior of shell
Every function works as specified in the assignment specs and have been modelled to have the same behavior as bash

## Limitations:
This shell was programmed on Mac, not sure if the paths will cause an issue on Windows or Linux operating systems
Try to have 'Region' set to `ca-central-1` otherwise `create_bucket` MIGHT NOT work. That being said, I've never needed to specify region in any part of my setup or execution of code