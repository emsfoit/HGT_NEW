# import zipfile
import pandas as pd

input_dir = "OAG/data/oag_raw/"
output_dir = "OAG/data/oag_raw/dummy/"
# move to pre/utils
def load_filter_by_time(file_name, sep="\t", max_size=10, time_key=None,
                   min_time=float("-inf"), max_time=float("inf"), chunksize=1000):
    iter_csv = pd.read_csv(file_name, sep=sep, iterator=True, chunksize=chunksize)
    # Check if there is a better way to do this one
    columns = iter_csv.get_chunk().columns.to_list()
    result = pd.DataFrame(columns=columns)
    for chunk in iter_csv:
      if len(result) > max_size:
        break
      x = chunk[(chunk[time_key] > min_time) & (chunk[time_key] < max_time)]
      result = pd.concat([result, x])
    result = result[:max_size]
    return result
# pass as an arg DF instead of file
def filter_with_merge(file_name, sep="\t", max_size=10, time_key=None, merge_on="", main_file):
  # TODO: read files as chunks
  df = pd.read_csv(file_name, sep=sep)
  columns = df.columns.to_list()
  df = df.merge(main_file, how="inner", on=merge_on)[columns]
  return df

# train_range = {t: True for t in graph.times if t != None and t < 2015}
# valid_range = {t: True for t in graph.times if t != None and t >= 2015  and t <= 2016}
# test_range  = {t: True for t in graph.times if t != None and t > 2016}

# Main File
# Papers_CS_20190919.tsv
file_name = "Papers_CS_20190919.tsv"
file_name = input_dir + file_name
train_data = load_filter_by_time(file_name=file_name, time_key="PublishYear", max_time=2015)
valid_data = load_filter_by_time(file_name=file_name, time_key="PublishYear", min_time=2014, max_time=2017)
test_data = load_filter_by_time(file_name=file_name, time_key="PublishYear", min_time=2016)
current_full_data = pd.concat([train_data, valid_data, test_data])
current_full_data.to_csv(output_dir + file_name, index=False, sep="\t")

# ['PaperId', 'Abstract']
file_name = "PAb_CS_20190919.tsv"
file_name = input_dir + file_name
data = filter_with_merge(file_name=file_name, merge_on="PaperId")
data.to_csv(output_dir + file_name, index=False, sep="\t")


# ['PaperId', 'FieldOfStudyId']
file_name = "PF_CS_20190919.tsv"
file_name = input_dir + file_name
data = filter_with_merge(file_name=file_name, merge_on="PaperId")
data.to_csv(output_dir + file_name, index=False, sep="\t")

# ['PaperId', 'ReferenceId']
file_name = "PR_CS_20190919.tsv"
file_name = input_dir + file_name
data = filter_with_merge(file_name=file_name, merge_on="PaperId")
data.to_csv(output_dir + file_name, index=False, sep="\t")



############# Notes ############# 

# ['PaperId', 'PublishYear', 'NormalizedTitle', 'VenueId', 'DetectedLanguage', 'DocType', 'EstimatedCitation']
# file_name="Papers_CS_20190919.tsv"

# ['PaperId', 'Abstract']
# file_name = "PAb_CS_20190919.tsv"

# ['PaperId', 'FieldOfStudyId']
# file_name = "PF_CS_20190919.tsv"

# ['PaperId', 'ReferenceId']
# file_name = "PR_CS_20190919.tsv"


# ['PaperSeqid', 'AuthorSeqid', 'AffiliationSeqid', 'AuthorSequenceNumber']
# file_name = "PAuAf_CS_20190919.tsv"


# Victors
# file_name = "vfi_vector.tsv"

