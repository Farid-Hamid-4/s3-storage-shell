from pathlib import Path


def passes_prelim_check(s3, __current_directory__):
    """
    Disable the user from using certain cloud functions at start of program and when root is empty
    """
    if (__current_directory__ == '/' and is_s3_empty(s3) == True):
        print('You must first create a bucket and then chlocn into before you can use this command.')
        return False
    elif (__current_directory__ == '/'):
        print('You must chlocn into an existing bucket before you can use this command.')
        return False
    return True
    

def get_bucket_name(user_path, __current_directory__):
    """
    Return the bucket name from the string
    """
    try:
        if (user_path.startswith('/') == True):
            path_list = list(Path(user_path).parts)
            if (len(path_list) > 1):
                return str(path_list[1])

        elif (len(Path(__current_directory__).parts) > 1):
            return str(Path(__current_directory__).parts[1])

        else:
            print('Invalid bucket name')

    except Exception as error:
        print(error)
    return ''


def is_s3_empty(s3):
    """
    Check if the s3 storage is empty
    """
    try:
        for bucket in s3.buckets.all():
            return False
    except Exception as error:
        print(error)
        return False
    return True


def is_bucket_empty(s3, bucket_name):
    """
    Check if the bucket is empty
    """
    try:
        for object in s3.Bucket(bucket_name).objects.all():
            return False
    except Exception as error:
        print(error)
        return
    return True


def bucket_exists(s3, bucket_name):
    """
    Check if the bucket exists
    """
    if ((s3.Bucket(bucket_name) in s3.buckets.all()) == False):
        return False
    return True


def object_exists(s3, cloud_path, __current_directory__):
    """
    Check if the object in the specified bucket exists
    """
    bucket_name = get_bucket_name(cloud_path, __current_directory__)
    object_key = str(Path(*Path(cloud_path).parts[2:]))

    try:
        for item in s3.Bucket(bucket_name).objects.all():
            if (object_key == item.key.strip("/")):
                return True
    except Exception as error:
        print(error)
        return
    return False


def is_cloud_folder_empty(s3, cloud_path, __current_directory__):
    """
    Check if a folder in a bucket is empty
    """
    bucket_name = get_bucket_name(cloud_path, __current_directory__)
    folder_name = str(Path(*Path(cloud_path).parts[2:])) + '/'
    count = s3.Bucket(bucket_name).objects.filter(Prefix=folder_name)
    if (len(list(count)) > 1):
        return False
    return True


def is_cloud_folder(s3, cloud_path, __current_directory__):
    """
    Checks if the path provided is a valid folder
    """
    bucket_name = get_bucket_name(cloud_path, __current_directory__)
    object_key = str(Path(*Path(cloud_path).parts[2:])) + '/'
    try:
        for item in s3.Bucket(bucket_name).objects.all():
            if (object_key == item.key):
                return True
    except Exception as error:
        print(error)
        return False
    return False


def cloud_folder_path_exists(s3, full_cloud_path, __current_directory__):
    """
    Check if the cloud folder path of the file exists
    """
    cloud_folder = str(Path(*Path(full_cloud_path).parts[:-1]))
    if (is_cloud_folder(s3, cloud_folder, __current_directory__) == False):
        return False
    return True


def print_buckets_short(s3):
    """
    Print all the buckets
    """
    for bucket in s3.buckets.all():
        print(bucket.name, end="\t")
    print('')
    return


def print_buckets_long(s3):
    """
    Print all the buckets along with their (type, size, permission)
    """
    for bucket in s3.buckets.all():
        permission = s3.BucketAcl(bucket.name).grants[0]['Permission']
        type = s3.BucketAcl(bucket.name).grants[0]['Grantee']['Type']
        size = "%.2f mb" % float(
            sum([object.size for object in s3.Bucket(bucket.name).objects.all()])/1000/1024)
        path = "/" + bucket.name
        date = bucket.creation_date
        print('{} - {} - {} - {} - {}'.format(permission, type, size, date, path))
    return


def print_objects_short(s3, cloud_path, __current_directory__):
    """
    Print all the objects in the specified bucket or folder
    """
    counter = 0
    bucket_name = get_bucket_name(str(cloud_path), __current_directory__)
    
    if(len(Path(cloud_path).parts) > 2):
        if(is_cloud_folder(s3, cloud_path, __current_directory__) == False):
            print('The cloud folder does not exist')
            return

        folder_path = '/'.join(Path(cloud_path).parts[2:]) + '/'
        
        for object in s3.Bucket(bucket_name).objects.filter(Prefix=folder_path):
            if (len(Path(object.key).parts) == len(Path(folder_path).parts) + 1):
                print(Path(object.key).name + '  ', end="")
                counter += 1

    else:
        for object in s3.Bucket(bucket_name).objects.all():
            if(len(Path(str(object.key)).parts) == 1):
                print((Path(object.key).name) + '  ', end="")
                counter += 1
                
    if(counter > 0): print('')
    return


def print_objects_long_bucket(s3, cloud_path, __current_directory__):
    """
    Print all the objects in the specified bucket along with the (type, size, permission)
    """
    bucket_name = get_bucket_name(str(cloud_path), __current_directory__)
    for object in s3.Bucket(bucket_name).objects.all():
        if (len(Path(str(object.key)).parts) == 1):
            permission = s3.ObjectAcl(
                bucket_name, object.key).grants[0]['Permission']
            type = s3.ObjectAcl(
                bucket_name, object.key).grants[0]['Grantee']['Type']
            size = 0
            size += object.size
            size = "%.2f mb" % float(size/1000/1024)
            date = object.last_modified
            path = Path(object.key).name
            print('{} - {} - {} - {} - {}'.format(permission,
                    type, size, date, path))
    return


def print_objects_long_folder(s3, cloud_path, __current_directory__):
    """
    Print all the objects in the specified bucket along with the (type, size, permission)
    """
    bucket_name = get_bucket_name(str(cloud_path), __current_directory__)

    folder_path = '/'.join(Path(cloud_path).parts[2:]) + '/'

    # Folder objects in long form
    for object in s3.Bucket(bucket_name).objects.filter(Prefix=folder_path):
        if (len(Path(object.key).parts) == len(Path(folder_path).parts) + 1):
            permission = s3.ObjectAcl( 
                bucket_name, object.key).grants[0]['Permission']
            type = s3.ObjectAcl(
                bucket_name, object.key).grants[0]['Grantee']['Type']
            size = 0
            size += object.size
            size = "%.2f mb" % float(size/1000/1024)
            date = object.last_modified
            path = Path(object.key).name
            print('{} - {} - {} - {} - {}'.format(permission, type, size, date, path))
    return
