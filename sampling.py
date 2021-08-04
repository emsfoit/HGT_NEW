# import zipfile
import pandas as pd

input_dir = "OAG/data/oag_raw/"
output_dir = "OAG/data/oag_raw/dummy/"
main_file_path = output_dir + "Papers_CS_20190919.tsv"

def load_filter_by_time(file_path, sep="\t", max_size=10, time_key=None, chunksize=1000, time=[]):
    iter_csv = pd.read_csv(file_path, sep=sep, iterator=True, chunksize=chunksize)
    columns = iter_csv.get_chunk().columns.to_list()
    result = pd.DataFrame(columns=columns)
    for elm in time:
      df = pd.DataFrame(columns=columns)
      
      min_time = elm['min_time'] if 'min_time' in elm else float("-inf")
      max_time = elm['max_time'] if 'max_time' in elm else  float("inf")
      for chunk in iter_csv:
        if len(df) > max_size:
          break
        filtered = chunk[(chunk[time_key] > min_time) & (chunk[time_key] < max_time)]
        df = pd.concat([df, filtered])
      df = df[:max_size]
      result = pd.concat([result, df])
    return result

def merge_csvfiles(f1=None, f2=None, sep="\t", max_size=10, merge_on=""):
  df1 = pd.read_csv(f1, sep=sep)
  df2 = pd.read_csv(f2, sep=sep)
  df = df1.merge(df2, how="inner", on=merge_on)
  return df

def filter_csv_by_att_from_another_csv(f1, f2, sep="\t", f2_on="", f1_on=""):
  df1 = pd.read_csv(f1, sep=sep)
  df2 = pd.read_csv(f2, sep=sep)
  common = df1.merge(df2, right_on=f2_on, left_on=f1_on)
  result = df1[df1[f1_on].isin(common[f1_on])]
  return result

class OAG_SAMPLES:
  def __init__(self, name, age):
    self.name = name
    self.age = age
    self.main_file_path = ""
  
  def run(self):
    self.filter_main_file()
    self.filter_pa_file()
    self.filter_pf_file()
    self.filter_pf_file()
    self.filter_pr_file()
    self.filter_PAuAf_file()


  def filter_main_file(self):
    file_name = "Papers_CS_20190919.tsv"
    main_file_path = input_dir + file_name
    current_full_data = load_filter_by_time(file_path=main_file_path, time_key="PublishYear", time=[{'max_time': 2015}, {'min_time': 2014, 'max_time': 2017}, {'min_time': 2016}])
    current_full_data.to_csv(output_dir + file_name, index=False, sep="\t")
    self.main_file_path = output_dir + file_name

  def filter_pa_file(self):
    # ['PaperId', 'Abstract']
    file_name = "PAb_CS_20190919.tsv"
    file_path = input_dir + file_name
    data = merge_csvfiles(f1=main_file_path, f2=file_path, merge_on="PaperId")
    data.loc[:, ['PaperId', 'Abstract']].to_csv(output_dir + file_name, index=False, sep="\t")

  def filter_pf_file(self):
    # ['PaperId', 'FieldOfStudyId']
    file_name = "PF_CS_20190919.tsv"
    file_path = input_dir + file_name
    data = merge_csvfiles(f1=main_file_path, f2=file_path, merge_on="PaperId")
    data.loc[:, ['PaperId', 'FieldOfStudyId']].to_csv(output_dir + file_name, index=False, sep="\t")

  def filter_pf_file(self):
    # FHierarchy_20190919.tsv
    # ['ChildFosId',	'ParentFosId',	'ChildLevel',	'ParentLevel']
    # check if (ChildFosId in ffl) and (ParentFosId in ffl):
    # Read ['PaperId', 'FieldOfStudyId'] from PF_CS_20190919 and save it in ffl
    ffl = {}
    file_name = "PF_CS_20190919.tsv"
    df = pd.read_csv(output_dir + file_name, sep="\t")
    for ind in df.index:
      ffl[df['FieldOfStudyId'][ind]] = True
    file_name = "FHierarchy_20190919.tsv"
    file_path = input_dir + file_name
    df = pd.read_csv(output_dir + file_name, sep="\t")
    df = df[(df["ChildFosId"].isin(ffl) & df["ParentFosId"].isin(ffl))]
    df.to_csv(output_dir + file_name, index=False, sep="\t")

  def filter_pr_file(self):
    # ['PaperId', 'ReferenceId']
    file_name = "PR_CS_20190919.tsv"
    file_path = input_dir + file_name
    data = merge_csvfiles(f1=main_file_path, f2=file_path, merge_on="PaperId")
    data.loc[:, ['PaperId', 'ReferenceId']].to_csv(output_dir + file_name, index=False, sep="\t")

  def filter_PAuAf_file(self):
    # ['PaperSeqid', 'AuthorSeqid', 'AffiliationSeqid', 'AuthorSequenceNumber']
    # affiliation_id in vfi_ids
    file_name = "PAuAf_CS_20190919.tsv"
    file_path = input_dir + file_name
    data = filter_csv_by_att_from_another_csv(f1=file_path, f2=main_file_path, sep="\t", f1_on="PaperId", f2_on="PaperSeqid")
    data.to_csv(output_dir + file_name, index=False, sep="\t")



# filter_main_file()
# filter_pa_file()
OAG_SAMPLES().run()


# !not useed
# ._SeqName_CS_20190919.tsv
# !not used
# Stats_CS_20190919.tsv
# author  5985759

# SeqName_CS_20190919.tsv
# if node_id in graph.node_forward[node_type]: !!!
# 465895  eureka  journal


# is it useful?
def merge_dataframes(df1=None, df2=None, merge_on=""):
  df = df1.merge(df2, how="inner", on=merge_on)
  return df
