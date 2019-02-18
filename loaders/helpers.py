import re
import zipfile
import datetime


def get_time_now(text=True, format='%Y-%m-%d %H:%M:%S'):
    if text is True:
        return datetime.datetime.now().strftime(format)
    else:
        return datetime.datetime.now()


def sql_merge_statement(dest_table, all_fields, key_fields):
    data_fields = list(set(all_fields).difference(key_fields))
    all_fields = map(lambda x: "[" + x + "]", all_fields)
    key_fields = map(lambda x: "[" + x + "]", key_fields)
    data_fields = map(lambda x: "[" + x + "]", data_fields)

    if len(key_fields) > 0:
        s = "MERGE " + dest_table + "\nUSING (\n\tVALUES(" + ','.join(map(lambda x: '?', all_fields)) + ")\n)"
        s = s + " AS src (" + ','.join(all_fields) + ")\n ON "
        s = s + ' AND '.join(map(lambda x: (dest_table + ".{c} = src.{c}").format(c=x), key_fields))
        s = s + "\nWHEN MATCHED THEN \n\tUPDATE SET " + ','.join(
            map(lambda x: "{c} = src.{c}".format(c=x), data_fields))
        s = s + "\nWHEN NOT MATCHED THEN \n\tINSERT (" + ','.join(all_fields) + ")"
        s = s + "\n\tVALUES (" + ','.join(map(lambda x: 'src.' + x, all_fields)) + ")\n;"

    else:
        s = "INSERT INTO " + dest_table + "(" + ','.join(all_fields) + ") VALUES (" + ','.join(
            map(lambda x: '?', all_fields)) + ")"
    return s


def unzip_matching_pattern(src, dest, regex):
    """Extract files have pattern.
    Args:
        src (string): where zip file located
        dest (string): where to save files after extracted
        regex (string): only extract files have pattern (regex). Eg: ".+/DATA/.+\.zip"
    """
    archive = zipfile.ZipFile(src)

    for file in archive.namelist():
        if re.match(regex, file):
            archive.extract(file, dest)

    archive.close()
