import configparser
import sys
import os
import boto3
import readline  # Enables arrow key functionality
from botocore.exceptions import ClientError
from pathlib import Path
from helpers import *


# Program Information
__author__ = "Farid Hamid"
__email__ = "fhamid@uoguelph.ca"
__student_id__ = "1067867"


# Allow the entire program to have read/write access to current directory, starting from root
__current_directory__ = '/'


def shell_command(s3, user_input):
    """
    Pass control over to another function/shell to perform a task or exit program
    :param p1: user_input (string)
    """
    tokenize = []
    if (user_input.strip() == 'exit' or user_input.strip() == 'quit'):
        exit()
    else:
        tokenize = user_input.split(" ")

    # Use a function dictionary to call desired functions
    if tokenize[0] in function_dict:
        function_dict[tokenize[0]](s3, tokenize)
    else:
        if (tokenize[0] == 'cd'):
            try:
                os.chdir(tokenize[1])
            except Exception as error:
                print(error)
        else:
            os.system(user_input)
    return


def locs3cp(s3, tokenize):
    """
    Copies a local file to a Cloud (S3) location
    :return on success: command prompt
    :return on failure: error message, e.g. "Unsuccessful copy"
    """
    try:
        global __current_directory__

        if(len(tokenize) != 3):
            if(len(tokenize) == 1): print('Unsuccessful copy, missing file operand')
            if(len(tokenize) == 2): print('Unsuccessful copy, missing destination file operand')
            return 1
        
        if (passes_prelim_check(s3, __current_directory__) == False):
            return 1

        # Check if the local file exists
        if ((Path(tokenize[1]).is_file()) == False):
            print('Unsuccessful copy, local file \'' + Path(tokenize[1]).name + '\' not found')
            return 1

        bucket_name = get_bucket_name(tokenize[2], __current_directory__)

        # Set path to be absolute
        if (tokenize[2].startswith('/')):
            path = Path(tokenize[2])
        else:
            path = Path(__current_directory__).joinpath(tokenize[2])
        
        # Check if user specified a file name for the cloud object
        if (len(path.parts) <= 2):
            print('Unsuccessful copy, file name for destination not provided')
            return 1

        # Check if the cloud folder the file that wants to be copied into exists
        if (len(path.parts) > 3):
            if (cloud_folder_path_exists(s3, path, __current_directory__) == False):
                print('Unsuccessful copy, the cloud folder \'' + Path(path).parts[-2] + '\' does not exist')
                return 1

        # Get the local path and cloud path
        local_path = tokenize[1]
        cloud_path = str(Path(*path.parts[2:]))

        # Check if the bucket exists or not
        if (bucket_exists(s3, bucket_name) == False):
            print('Unsuccessful copy, bucket \'' + bucket_name + '\' does not exist')
            return 1

        s3.Bucket(bucket_name).upload_file(local_path, cloud_path)
    
    except Exception as error:
        print('Unsuccessful copy,', error)
        return 1

    return 0


def s3loccp(s3, tokenize):
    """
    Copies a Cloud object to a local file system location
    :return on success: command prompt
    :return on failure: error message, e.g. "Unsuccessful copy"
    """
    try:
        global __current_directory__

        if(len(tokenize) != 3):
            if(len(tokenize) == 1): print('Unsuccessful copy, missing file operand')
            if(len(tokenize) == 2): print('Unsuccessful copy, missing source file operand')
            return 1

        if (passes_prelim_check(s3, __current_directory__) == False):
            return 1

        bucket_name = get_bucket_name(tokenize[1], __current_directory__)

        if (tokenize[1].startswith('/')):
            path = Path(tokenize[1])
        else:
            path = Path(__current_directory__).joinpath(tokenize[1])

        # Check if user specified a file name for the cloud object
        if (len(path.parts) <= 2):
            print('Unsuccessful copy, file name for destination not provided')
            return 1

        # Check if the cloud folder the file that wants to be copied into exists
        if (len(path.parts) > 3):
            if (cloud_folder_path_exists(s3, path, __current_directory__) == False):
                print('Unsuccessful copy, the cloud folder \'' + Path(path).parts[-2] + '\' does not exist')
                return 1

        # Check if bucket exists
        if (bucket_exists(s3, bucket_name) == False):
            print('Unsuccessful copy, bucket \'' + bucket_name + '\' does not exist')
            return 1

        # Check if object exists
        if (object_exists(s3, str(path), __current_directory__) == False):
            print('Unsuccessful copy, object \'' + Path(path).name + '\' does not exist')
            return 1

        s3.Bucket(bucket_name).download_file(Path(tokenize[1]).name, tokenize[2])

    except Exception as error:
        print('Unsuccesful copy,', error)
        return 1

    return 0


def create_bucket(s3, tokenize):
    """
    Creates a bucket in the user's S3 space following naming conventions for S3 buckets
    :return on success: command prompt
    :return on failure: e.g. "Cannot create bucket"
    """
    try:
        global __current_directory__
        bucket_name = ''

        if(len(tokenize) == 1): 
            print('Cannot create bucket, missing source bucket name')
            return 1

        # Absolute Path
        if (tokenize[1].startswith('/')):
            if (len(Path(tokenize[1]).parts) > 2):
                print('Cannot create bucket, invalid pathname')
                return 1
            else:
                bucket_name = Path(tokenize[1]).parts[1]

        # Relative path only allowed when __current_directory__ is '/'
        elif (tokenize[1].startswith('/') == False and __current_directory__ == '/'):
            if (len(Path(tokenize[1]).parts) > 1):
                print('Cannot create bucket, invalid pathname')
                return 1
            else:
                bucket_name = Path(tokenize[1]).parts[0]

        # Else cannot create bucket if you're not in root and you're using a relative path
        else:
            print('Cannot create bucket if you\'re not in root and you\'re using a relative path')
            return 1

        # Check if the bucket already exists
        if (bucket_exists(s3, bucket_name) == True):
            print('Cannot create bucket, \'' + bucket_name + '\' already exists')
            return 1

        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'ca-central-1'})

    except Exception as error:
        print('Cannot create bucket,', error)
        return 1

    return 0


def create_folder(s3, tokenize):
    """
    Creates a directory or folder in an existing S3 bucket
    :return on success: command prompt
    :return on failure: error message, e.g. "Cannot create folder"
    """
    try:
        client = boto3.client(service_name='s3', region_name='ca-central-1')
        global __current_directory__
        
        if(len(tokenize) == 1): 
            print('Cannot create folder, missing source folder name')
            return 1
        
        if (passes_prelim_check(s3, __current_directory__) == False):
            return 1

        if (tokenize[1].startswith('/')):
            path = str(Path(tokenize[1]))
        else:
            path = str(Path(__current_directory__).joinpath(tokenize[1]))
        
        bucket_name = get_bucket_name(path, __current_directory__)
        folder_name = str(Path(*Path(path).parts[2:])) + '/'

        if (bucket_exists(s3, bucket_name) == True):
            if (is_cloud_folder(s3, path, __current_directory__) == True):
                print('Cannot create folder, \'' + Path(path).name + '\' already exists')
                return 1
            
            if (len(Path(path).parts) > 3):
                if (is_cloud_folder(s3, str(Path(*Path(path).parts[:-1])), __current_directory__) == False):
                    print('Cannot create folder, \'' + Path(path).parts[-2] + '\' does not exist')
                    return 1
            
            client.put_object(Bucket=bucket_name, Body='', Key=folder_name)
        
        else:
            print('Cannot create folder, bucket \'' + bucket_name + '\' does not exist')
            return 1
    
    except Exception as error:
        print('Cannot create folder,', error)
        return 1

    return 0


def chlocn(s3, tokenize):
    """
    Changes the current working directory in your S3 space
    :return on success: command prompt
    :return on failure: error message, e.g. "Cannot change folder"
    """
    try:
        global __current_directory__

        if(len(tokenize) == 1): 
            print('Cannot change folder, missing folder to change to')
            return 1
        
        # Create a path list
        path_list = list()
        if(__current_directory__ != '/'):
            path_list = list(Path(__current_directory__).parts[1:])

        if(tokenize[1] == '/' or tokenize[1] == '~'):
            __current_directory__ = '/'
            return 0
        else:
            if(tokenize[1].startswith('/')):
                path_list = list()
            for item in Path(tokenize[1]).parts:
                if(item == '..' and len(path_list) > 0):
                    path_list.pop()
                elif(item.count('.') == len(item) and len(item) > 2):
                    print('Cannot change folder, \'' + str(item) + '\' is not a valid directory')
                    return 1
                elif(item != '/'):
                    path_list.append(item)
        
        path = '/' + '/'.join(path_list)
        
        # check if the bucket and folder exists
        if(len(Path(path).parts) > 2):
            if(bucket_exists(s3, str(Path(path).parts[1])) == False):
                print('Cannot change folder, bucket \'' + Path(path).parts[1] + '\' does not exist')
                return 1
            if(is_cloud_folder(s3, path, __current_directory__) == False):
                print('Cannot change folder, invalid cloud path')
                return 1
        
        __current_directory__ = str(path)

    except Exception as error:
        print('Cannot change folder,', error)
        return 1

    return 0


def cwlocn(s3, tokenize):
    """
    Displays the current working location/directory
    :return on success: <bucket name>:<full pathname of directory>
    :return on failure: error message, e.g. "Cannot access location in S3 space"
    """
    global __current_directory__
    print(__current_directory__)
    return 0


def llist(s3, tokenize):
    """
    List will show either a short or long form of the contents of your current working directory or a specified S3 location (including "/")
    :return on sucess: displays the S3 location's content similar to the Unix ls command
    :return on failure: error message, e.g. "Cannot list contents of this S3 location"
    """
    try:
        global __current_directory__

        # 'list' If in the root, print buckets, otherwise print objects of current bucket / folder.
        if (len(tokenize) == 1):
            if(__current_directory__ == '/'):
                print_buckets_short(s3)
            else:
                print_objects_short(s3, __current_directory__, __current_directory__)

        # 'list /' print short form of buckets at root
        elif (len(tokenize) == 2 and tokenize[1] == '/'):
            print_buckets_short(s3)
        
        # 'list -l' of the current directory. Check if the current directory is a bucket or folder through its length
        elif (len(tokenize) == 2 and tokenize[1] == '-l'):
            if(__current_directory__ == '/'):
                print_buckets_long(s3)
            elif(len(Path(__current_directory__).parts) == 2):
                print_objects_long_bucket(s3, __current_directory__, __current_directory__)
            else:
                print_objects_long_folder(s3, __current_directory__, __current_directory__)
        
        # 'list /some/path' print the contents of S3 location based on user path 
        elif (len(tokenize) == 2 and tokenize[1] != '-l' and tokenize[1] != '/'):
            if (tokenize[1].startswith('/')):
                path = str(Path(tokenize[1]))
            else:
                path = str(Path(__current_directory__).joinpath(tokenize[1]))

            if(is_cloud_folder(s3, path, __current_directory__) == False):
                print('Cannot list contents of this S3 location, the cloud folder does not exist')

            print_objects_short(s3, path, __current_directory__)

        # 'list -l /' print long form of buckets at root
        elif (len(tokenize) == 3 and tokenize[1] == '-l' and tokenize[2] == '/'):
            print_buckets_long(s3)

        # 'list -l /some/path' print long form based on if user specifies a bucket or folder
        elif (len(tokenize) == 3 and tokenize[1] == '-l' and tokenize[2] != '/'):
            if(tokenize[2].startswith('/')):
                path = str(Path(tokenize[2]))
            else:
                path = str(Path(__current_directory__).joinpath(tokenize[2]))
            
            if(is_cloud_folder(s3, path, __current_directory__) == False):
                print('Cannot list contents of this S3 location, the cloud folder does not exist')

            if(len(Path(path).parts) > 2):
                print_objects_long_folder(s3, path, __current_directory__)
            else:
                print_objects_long_bucket(s3, path, __current_directory__)

    except Exception as error:
        print('Cannot list contents of this S3 location,', error)
        return 1
    return 0


def s3copy(s3, tokenize):
    """
    Copy an object from one S3 location to another
    :return on sucess: command prompt
    :return on failure: error message, e.g. "Cannot perform copy"
    """
    try:
        global __current_directory__

        if(len(tokenize) != 3):
            if(len(tokenize) == 1): print('Cannot perform copy, missing file operand')
            if(len(tokenize) == 2): print('Cannot perform copy, missing destination file operand')
            return 1

        if (passes_prelim_check(s3, __current_directory__) == False):
            return 1

        if (tokenize[1].startswith('/')):
            source_path = str(Path(tokenize[1]))
        else:
            source_path = str(Path(__current_directory__).joinpath(tokenize[1]))

        source_bucket_name = get_bucket_name(str(source_path), __current_directory__)
        source_key = str(Path(*source_path.parts[2:]))

        source = {
            'Bucket': source_bucket_name,
            'Key': source_key
        }

        if (tokenize[2].startswith('/')):
            destination_path = str(Path(tokenize[2]))
        else:
            destination_path = str(Path(__current_directory__).joinpath(tokenize[2]))

        destination_bucket_name = get_bucket_name(destination_path, __current_directory__)
        destination_key = str(Path(*destination_path.parts[2:]))

        if(bucket_exists(s3, source_bucket_name) == False):
            print('Cannot perform copy, bucket \'' + source_bucket_name + '\' does not exist')
            return 1

        if (bucket_exists(s3, destination_bucket_name) == False):
            print('Cannot perform copy, bucket \'' + destination_bucket_name + '\' does not exist')
            return 1

        if(object_exists(s3, source_path, __current_directory__) == False):
            print('Cannot perform copy, source object does not exist')
            return 1

        if(is_cloud_folder(s3, destination_path, __current_directory__) == True):
            if(is_cloud_folder_empty(s3, destination_path, __current_directory__) == False):
                print('Cannot perform copy, \'' + Path(destination_path).name + '\' is not an empty folder')
                return 1

        if(len(destination_path.parts) > 3):
            if(cloud_folder_path_exists(s3, destination_path, __current_directory__) == False):
                print('Cannot perform copy, destination path does not exist')
                return 1

        s3.meta.client.copy(source, destination_bucket_name, destination_key)

    except Exception as error:
        print('Cannot perform copy,', error)
        return 1

    return 0    


def s3delete(s3, tokenize):
    """
    Delete an object (directories included but only if they are empty). This command will not delete buckets
    :return on sucess: command prompt 
    :return on failure: error message, e.g. "Cannot perform delete"
    """
    try:
        global __current_directory__

        if(len(tokenize) == 1):
            print('Cannot perform delete, missing file operand')
            return 1

        if (passes_prelim_check(s3, __current_directory__) == False):
            return 1

        bucket_name = get_bucket_name(tokenize[1], __current_directory__)

        # Set path to absolute
        if (tokenize[1].startswith('/')):
            path = Path(tokenize[1])
        else:
            path = Path(__current_directory__).joinpath(tokenize[1])
        
        object_key = str(Path(*path.parts[2:]))

        # Check if the bucket exists
        if ((bucket_exists(s3, bucket_name)) == False):
            print('Cannot perform delete, bucket: \'' + bucket_name + '\' does not exist')
            return 1

        # Check if the object exists
        if ((object_exists(s3, str(path), __current_directory__)) == False):
            print('Cannot perform delete, object: \'' + Path(path).name + '\' does not exist')
            return 1
        
        # Check if the folder to be deleted exists and is empty
        if (is_cloud_folder(s3, str(path), __current_directory__) == True):
            if (is_cloud_folder_empty(s3, str(path), __current_directory__) == False):
                print('Cannot perform delete, \'' + Path(path).name + '\' is not empty')
                return 1
            elif (is_cloud_folder_empty(s3, str(path), __current_directory__) == True):
                object_key = object_key + '/'

        s3.Object(bucket_name, object_key).delete()
    
    except Exception as error:
        print('Cannot perform delete,', error)
        return 1

    return 0


def delete_bucket(s3, tokenize):
    """
    Delete a bucket if it is empty. You cannot delete the bucket that you are currently in
    :return on success: command prompt
    :return on failure: error message, e.g. "Cannot delete bucket"
    """
    try:
        global __current_directory__

        if(len(tokenize) == 1):
            print('Cannot delete bucket, missing bucket name')
            return 1
        
        if (passes_prelim_check(s3, __current_directory__) == False):
            return 1

        bucket_name = ''

        # Absolute path
        if (tokenize[1].startswith('/')):
            if (len(Path(tokenize[1]).parts) > 2):
                print('Cannot delete bucket, invalid pathname')
                return 1
            else:
                bucket_name = Path(tokenize[1]).parts[1]

        # Relative path
        elif (tokenize[1].startswith('/') == False and __current_directory__ == '/'):
            if (len(Path(tokenize[1]).parts) > 1):
                print('Cannot delete bucket, invalid pathname')
                return 1
            else:
                bucket_name = Path(tokenize[1]).parts[0]

        # Else cannot delete bucket if you're not in root and you're using a relative path
        else:
            print('Cannot delete bucket, you are not in root')
            return 1

        if ((bucket_exists(s3, bucket_name)) == False):
            print('Cannot delete bucket, bucket \'' + bucket_name + '\' does not exist')
            return 1

        if ((is_bucket_empty(s3, bucket_name)) == False):
            print('Cannot delete bucket, \'' + bucket_name + '\' must be empty before deleting')
            return 1

        if (bucket_name == (Path(__current_directory__).parts[1])):
            print('Cannot delete a bucket you are currently in')
            return 1

        s3.Bucket(bucket_name).delete()

    except Exception as error:
        print('Cannot delete bucket,', error)
        return 1

    return 0


function_dict = {
    'locs3cp': locs3cp,
    's3loccp': s3loccp,
    'create_bucket': create_bucket,
    'create_folder': create_folder,
    'chlocn': chlocn,
    'cwlocn': cwlocn,
    'list': llist,
    's3copy': s3copy,
    's3delete': s3delete,
    'delete_bucket': delete_bucket
}


def main():
    """
    Authenticate AWS with S3 and launch shell
    """
    config = configparser.ConfigParser()
    config.read("S5-S3.conf")
    access_key = config['default']['aws_access_key_id']
    secret_key = config['default']['aws_secret_access_key']

    # Preliminary welcome message
    print('Welcome to the AWS S3 Storage Shell (S5)')

    # Indicating which service to use and authorizing
    try:
        session = boto3.Session(region_name='ca-central-1', 
                                aws_access_key_id=access_key,
                                aws_secret_access_key=secret_key)
        sts = session.client('sts')
        sts.get_caller_identity()
        s3 = boto3.resource(service_name='s3', region_name='ca-central-1')
        print('You are now connected to your S3 storage')
    except ClientError:
        print('You could not be connected to your S3 storage')
        print('Please review procedures for authenticating your account on AWS S3')
        sys.exit()

    # Shell infinite loop
    while True:
        shell_command(s3, input('S5> '))


if __name__ == "__main__":
    main()
