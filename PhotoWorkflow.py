#
# Simple photo workflow.  Currently, all it does is take photos and videos from a single incoming folder and
# move them into other folders named by month taken.
#

import os
from PIL import Image
from PIL.ExifTags import TAGS
import codecs

PILE_FOLDERS = [
    os.path.expanduser("~/Pictures/PhotoSync/Rainier/Camera Roll"),
    # exiftool can't handle unicode filenames; must use the shortened form
    # See http://u88.n24.queensu.ca/exiftool/forum/index.php?topic=5210.0
    os.path.expanduser("~/Pictures/PhotoSync/Rainier/D3A5~1"),
    #os.path.expanduser("~/Videos/From Backup"),
    "I:\\DCIM"
]
MONITORED_EXTENSIONS = (".jpg")
INCOMING_FOLDER = os.path.expanduser("~/Pictures/Incoming")
INCOMING_VIDEOS_FOLDER = os.path.expanduser("~/Videos/Incoming")
INCOMING_DATE_FORMAT = "%Y-%m"
EXIFTOOL_PATH = os.path.expanduser("~/bin/exiftool.exe")

def get_exif(fn):
    ret = {}
    i = Image.open(fn)
    info = i._getexif()
    for tag, value in info.items():
        decoded = TAGS.get(tag, tag)
        ret[decoded] = value
    return ret

def _get_pile_files(folder):
    return [os.path.join(root, name)
                 for root, dirs, files in os.walk(folder)
                 for name in files]

def _get_earliest_date(fn):
    import time
    import datetime
    raw_name, ext = os.path.splitext(fn)
    if ext.lower() == ".jpg" or ext.lower() == ".jpeg":
        exif = get_exif(fn)
        if 'DateTimeOriginal' in exif.keys():
            return "exif", datetime.datetime.strptime(exif['DateTimeOriginal'], "%Y:%m:%d %H:%M:%S")

    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fn))
    ctime = datetime.datetime.fromtimestamp(os.path.getctime(fn))
    if mtime < ctime:
        return "mtime", mtime
    else:
        return "ctime", ctime

def _safe_move_file(src, dest):
    """
    Moves a file without overwriting any existing file in dest.  dest must be a directory.
    :param src:
    :param dest:
    :return:
    """
    import shutil

    try:
        # shutil.move will overwrite dest if it it's a file.  This is dangerous, but can't be changed.
        if os.path.exists(dest) and not os.path.isdir(dest):
            raise Exception(
                "The destination folder {0} seems to already exist as a file; please investigate".format(dest))

        os.makedirs(dest, exist_ok=True)

        if not os.path.isdir(dest):
            raise Exception("Something's gone wrong with the creation of the destionation folder {0}; please investigate".format(dest))

        shutil.move(src, dest)
    except shutil.Error as e:
        base_name, ext = os.path.splitext(os.path.basename(src))
        for i in range(1, 100):
            new_dest = os.path.join(dest, "{0} ({1}){2}".format(base_name, i, ext))
            if not os.path.exists(new_dest):
                # This isn't totally safe, because the dest file could be created in between the check for
                # existence and the move.  But it's close enough.
                shutil.move(src, new_dest)
                break

def _datetime_from_exiftool(date_string):
    """
    Exiftool uses YYYY:MM:dd, which confuses dateutil.parser.  Change this to YYYY-MM-dd
    :param date_string:
    :return:
    """
    import dateutil.parser
    return dateutil.parser.parse(date_string[0:10].replace(':', '-') + date_string[10:])

def _duration_from_exiftool(duration_string):
    """
    Parses the duration from exiftool and returns the equivalent timedelta.
    :param duration_string:
    :return:
    """
    import dateutil.parser
    import datetime
    duration_datetime = dateutil.parser.parse(duration_string)  # This parses strings like "0:53" as 53 seconds after midnight, today
    return duration_datetime - datetime.datetime.combine(duration_datetime.date(), datetime.time(0))

def _get_video_creation_date(filename):
    """
    Returns the date from a video whose metadata can be queried with exiftool.  Attempts to get the date in local time
    at the location where the movie was taken.
    :param filename:
    :return:
    """
    import json
    import subprocess
    import codecs
    import dateutil.parser
    import dateutil.tz
    import datetime

    metadata_bytes = subprocess.check_output([EXIFTOOL_PATH, '-j', '-charset', 'UTF8', filename], stderr=subprocess.STDOUT)
    metadata_string = metadata_bytes.decode("utf-8")
    metadata = json.loads(metadata_string)[0]

    duration = _duration_from_exiftool(metadata['Duration'])

    # For iPhone videos, this uses the local time zone at the time the video was recorded.
    if 'CreationDate' in metadata:
        md_creationdate = _datetime_from_exiftool(metadata['CreationDate'])
        print("{0}: CreationDate={1}".format(os.path.basename(filename), md_creationdate))
        return md_creationdate

    if 'CreateDate' in metadata:
        md_ctime = _datetime_from_exiftool(metadata['CreateDate'])
        if md_ctime.tzinfo is None:
            md_ctime = md_ctime.replace(tzinfo=dateutil.tz.tzutc()) # Assume UTC

        file_mtime = _datetime_from_exiftool(metadata['FileModifyDate'])
        # Standard recommends that this be in UTC; let's check if it's close enough to the file modify date.
        # Account for the time the video was being taken and being written to disk
        mtime_as_utc = file_mtime.astimezone(dateutil.tz.tzutc())

        if abs((mtime_as_utc - md_ctime).total_seconds()) < duration.total_seconds() * 2 + 20:
            print("{0}: CreateDateAsUTC={1}".format(os.path.basename(filename), md_ctime.astimezone(dateutil.tz.tzlocal())))
            return md_ctime.astimezone(file_mtime.tzinfo)
            # Match! Go ahead and return the UTC time as local time.

        # What if we assume that CreateDate has the same time zone as file_mtime?
        md_ctime_as_local = md_ctime.replace(tzinfo=file_mtime.tzinfo)

        if abs((md_ctime_as_local - file_mtime).total_seconds()) < duration.total_seconds() * 2 + 30:
            print("{0}: CreateDateAsLocal={1}".format(os.path.basename(filename), md_ctime_as_local))
            return md_ctime_as_local

        print("{0}: Dates: {1}, {2}", filename, md_ctime, file_mtime)
        raise Exception("Couldn't figure out the relationship between CreateDate and file modification time")

    else:
        return None



def _get_file_action(fn):
    import time

    base_name, ext = os.path.splitext(fn)
    ext = ext.lower()

    media_files = (".jpg", ".jpeg", ".png", ".gif")

    if ext in media_files:
        date_src, my_date = _get_earliest_date(fn)
        return {'action': 'move', 'src': fn, 'dest': os.path.join(INCOMING_FOLDER, my_date.strftime(INCOMING_DATE_FORMAT))}

    elif ext in (".mov", ".mpeg", ".mp4"):
        my_date = _get_video_creation_date(fn)
        return  {'action': 'move', 'src': fn, 'dest': os.path.join(INCOMING_VIDEOS_FOLDER, my_date.strftime(INCOMING_DATE_FORMAT))}

    else:
        return {'action': 'ignore', 'src': fn}


actions = []
for folder in PILE_FOLDERS:
    if os.path.exists(folder):
        actions.extend([_get_file_action(fn) for fn in _get_pile_files(folder)])
    else:
        print("Folder %s doesn't exist; skipping" % folder)

moves = [a for a in actions if a['action'] == 'move']
ignores = [a for a in actions if a['action'] == 'ignore']

print("Preparing to process files: {0} move(s), {1} ignored".format(len(moves), len(ignores)))

with codecs.open("a.log", 'a', 'utf-8') as file:
    for fn in actions:
        if fn['action'] == 'move':
            file.write("{0}: {1} -> {2}\n".format(fn['action'], fn['src'], fn['dest']))
            _safe_move_file(fn['src'], fn['dest'])
        else:
            file.write("{0}: {1}\n".format(fn['action'], fn['src']))

print("Done.")