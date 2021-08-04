import pandas as pd
import bz2
import json

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

def merge_csvfiles(f1=None, f2=None, sep="\t", merge_on=""):
  df1 = pd.read_csv(f1, sep=sep)
  df2 = pd.read_csv(f2, sep=sep)
  df = df1.merge(df2, how="inner", on=merge_on)
  return df

def filter_csv_by_att_from_another_csv(f1, f2, sep="\t", f2_on="", f1_on=""):
  df1 = pd.read_csv(f1, sep=sep)
  df2 = pd.read_csv(f2, sep=sep)
  common = df1.merge(df2, right_on=f1_on, left_on=f2_on)
  result = df1[df1[f2_on].isin(common[f2_on])]
  return result

class OAG_SAMPLES:
  def __init__(self):
    # Take as params
    self.time = [{'max_time': 2015}, {'min_time': 2014, 'max_time': 2017}, {'min_time': 2016}]
    self.max_size = 10000
    self.input_dir = "OAG/data/oag_raw/"
    self.output_dir = "OAG/data/oag_raw/dummy/"
    self.main_file_path = self.output_dir + "Papers_CS_20190919.tsv"

  def run(self):
    self.filter_main_file()
    self.filter_pa_file()
    self.filter_pf_file()
    self.filter_fh_file()
    self.filter_pr_file()
    self.filter_PAuAf_file()
    self.filter_vfi_file
    self.filter_seq_file


  def filter_main_file(self):
    file_name = "Papers_CS_20190919.tsv"
    current_full_data = load_filter_by_time(file_path=self.input_dir + file_name, time_key="PublishYear", time=self.time, max_size=self.max_size)
    current_full_data.to_csv(self.output_dir + file_name + ".gz", index=False, sep="\t", compression='gzip')

  def filter_pa_file(self):
    # ['PaperId', 'Abstract']
    file_name = "PAb_CS_20190919.tsv"
    file_path = self.input_dir + file_name
    data = merge_csvfiles(f1=main_file_path, f2=file_path, merge_on="PaperId")
    data.loc[:, ['PaperId', 'Abstract']].to_csv(self.output_dir + file_name+".gz", index=False, sep="\t", compression='gzip')

  def filter_pf_file(self):
    # ['PaperId', 'FieldOfStudyId']
    file_name = "PF_CS_20190919.tsv"
    file_path = self.input_dir + file_name
    data = merge_csvfiles(f1=main_file_path, f2=file_path, merge_on="PaperId")
    data.loc[:, ['PaperId', 'FieldOfStudyId']].to_csv(self.output_dir + file_name+".gz", index=False, sep="\t", compression='gzip')

  def filter_fh_file(self):
    ffl = {}
    file_name1 = "PF_CS_20190919.tsv"
    df = pd.read_csv(self.output_dir + file_name1, sep="\t")
    for ind in df.index:
      ffl[df['FieldOfStudyId'][ind]] = True
    file_name = "FHierarchy_20190919.tsv"
    file_path = self.input_dir + file_name
    df = pd.read_csv(file_path, sep="\t")
    df = df[(df["ChildFosId"].isin(ffl) & df["ParentFosId"].isin(ffl))]
    df.to_csv(self.output_dir + file_name+".gz", index=False, sep="\t", compression='gzip')

  def filter_pr_file(self):
    # ['PaperId', 'ReferenceId']
    file_name = "PR_CS_20190919.tsv"
    file_path = self.input_dir + file_name
    data = merge_csvfiles(f1=main_file_path, f2=file_path, merge_on="PaperId")
    data.loc[:, ['PaperId', 'ReferenceId']].to_csv(self.output_dir + file_name+".gz", index=False, sep="\t", compression='gzip')

  def filter_PAuAf_file(self):
    # ['PaperSeqid', 'AuthorSeqid', 'AffiliationSeqid', 'AuthorSequenceNumber']
    file_name = "PAuAf_CS_20190919.tsv"
    file_path = self.input_dir + file_name
    data = filter_csv_by_att_from_another_csv(f1=file_path, f2=main_file_path, sep="\t", f1_on="PaperId", f2_on="PaperSeqid")
    data.to_csv(self.output_dir + file_name+".gz", index=False, sep="\t", compression='gzip')
  
  def filter_vfi_file(self):
    # Todo: Find a way to filter this file
    file_name = "vfi_vector.tsv"
    file_path = self.input_dir + file_name
    df = pd.read_csv(file_path, sep="\t")
    df.to_csv(self.output_dir + file_name+".gz", index=False, sep="\t", compression='gzip')

  def filter_seq_file(self):
    # Todo: Find a way to filter this file
    file_name = "SeqName_CS_20190919.tsv"
    file_path = self.input_dir + file_name
    df = pd.read_csv(file_path, sep="\t")
    df.to_csv(self.output_dir + file_name+".gz", index=False, sep="\t", compression='gzip')

class REDDIT_SAMPLES:
  def __init__(self):
    self.max_size = 10000
    self.input_dir = "OAG/data/reddit_raw/"
    self.output_dir = "OAG/data/reddit_raw/dummy/"

  def run(self):
    posts = self.filter_posts()
    self.filter_comments(posts)

  def filter_posts(self):
    file_name = "RS_2011-01.bz2"
    file_path = self.input_dir + file_name
    posts_data = []
    with bz2.open(file_path, "rt") as bzinput:
      for i, line in enumerate(bzinput):
          if i == self.max_size+1:
              break
          try:
            post = json.loads(line)
            post_id = post.get('name', '')
            post_title = post.get('title', '')
            post_created_utc = post.get('created_utc', '')
            author = post.get('author', '')
            subreddit_id = post.get('subreddit_id', '')
            subreddit = post.get('subreddit', '')
            if (post_id == '' or post_title == '' or post_created_utc == '' or len(post_title.split()) < 2):
                continue
            if (author == '' or subreddit_id == '' or subreddit == ''):
                continue
            posts_data.append(
                [post_id, post_title, post_created_utc, author, subreddit_id, subreddit])
          except:
            print("could not load line")
      posts_df = pd.DataFrame(posts_data, columns=[
                        'post_id', 'post_title', 'post_created_utc', 'author', 'subreddit_id', 'subreddit'])
    posts_df = posts_df.replace({';': ''}, regex=True)
    posts_df.to_csv(self.output_dir+"RS_2011-01.csv", sep=";", index=False)
    return posts_df

  def filter_comments(self, posts):
    file_name = "RC_2011-01.bz2"
    file_path = self.input_dir + file_name
    comments_data = []
    # Get all comments
    with bz2.open(file_path, "rt") as bzinput:
        for i, line in enumerate(bzinput):
          try:
            comment = json.loads(line)
            comment_id = comment.get('name', '')
            comment_body = comment.get('body', '')
            comment_created_utc = comment.get('created_utc', '')
            parent_id = comment.get('parent_id', '')
            author = comment.get('author', '')

            if (comment_id == '' or comment_body == '' or comment_created_utc == '' or parent_id == '' or author == ''):
                continue
            comments_data.append(
                [comment_id, comment_body, comment_created_utc, parent_id, author])
          except:
            print("could not load line")
    comments_df = pd.DataFrame(comments_data, columns=[
                              'comment_id', 'comment_body', 'comment_created_utc', 'parent_id', 'author'])

    # get all post ids, and comment_ids that have posts in the data
    post_ids = posts['post_id'].drop_duplicates().to_list()
    comment_ids_with_post = comments_df[comments_df['parent_id'].isin(
        post_ids)]['comment_id'].drop_duplicates().to_list()

    available_parent_ids = post_ids + comment_ids_with_post
    # Keep the comments that have a parent(post or comment) in the data
    comments_df = comments_df[comments_df['parent_id'].isin(available_parent_ids)]
    comments_df['post_id'] = comments_df[comments_df['parent_id'].str.contains(
        't3_')]['parent_id']
    comments_df['comment_parent_id'] = comments_df[comments_df['parent_id'].str.contains(
        't1_')]['parent_id']
    comments_df = comments_df.drop(columns=['parent_id'])

    # remove ";" from the data to use it as a separator in the output file
    comments_df = comments_df.replace({';': ''}, regex=True)
    comments_df.to_csv(self.output_dir+file_name, sep=";", index=False)



# OAG_SAMPLES().run()
REDDIT_SAMPLES().run()




# !not useed
# ._SeqName_CS_20190919.tsv
# !not used
# Stats_CS_20190919.tsv
# author  5985759

# SeqName_CS_20190919.tsv

# is it useful?
def merge_dataframes(df1=None, df2=None, merge_on=""):
  df = df1.merge(df2, how="inner", on=merge_on)
  return df
