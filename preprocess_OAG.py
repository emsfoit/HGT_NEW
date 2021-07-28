import argparse
from transformers import *
from pyHGT.data import *
from itertools import islice
from helpers import *
import bz2
import json
import dill
import torch
# Globale Variables
input_dir = "data/oag_raw"
output_dir = "data/oag_output"

cuda = 0
max = 100
posts_filename = 'RS_2011-01.bz2'
posts_filename = f'{os.path.dirname(__file__)}/{input_dir}/{posts_filename}'

comments_filename = "RC_2011-01.bz2"
comments_filename = f'{os.path.dirname(__file__)}/{input_dir}/{comments_filename}'

result_folder = f'{os.path.dirname(__file__)}/{output_dir}'


# convertBz2FileToDictArray (Node)
def convertBz2FileToDictArray(file_name, max=None, keys_tupels=None, extra_attributes=None, id="suthor_id", type="", random_id=False):
  if(not id or not type): 
    print("params are invalid")
    return
  if random_id:
    random_number = 100000
  data_dict =  {}
  with bz2.open(file_name, "rt",encoding="utf8") as bzinput:
      file = islice(enumerate(bzinput),0, max) if max  else enumerate(bzinput)
      for i, line in file:
        try:
          row = json.loads(line)
          node = {}
          node["type"] = type
          valid = True
          for key in keys_tupels:
            val = row.get(key[1], "")
            if not val:
              valid = False
              break
            node[key[0]] = val
          if valid:
            id_key = row.get(id) if not random_id else random_number + i
            if id_key in data_dict.keys():
              print("key already exist ", id_key)
            data_dict[id_key] = node
          else:
            print("Line is invalid", i)
        except:
          print("could not load line:", i)
      return data_dict

def add_edge(graph=object, nodes={}, targets={}, through="", relation_type="", time_key=""):
  time_val = 0
  for elm in nodes.values():
    target_id = elm.get(through, "")
    target = targets.get(target_id, "")
    if target:
      if time_key:
        try:
          time_val = int(elm.get(time_key, ""))
        except:
          time_val = 0
      print("Adding Edge")
      graph.add_edge(elm, target, time=time_val, relation_type= relation_type)
    else:
      print("No Edge Matched")

# Ex: Repetition
def add_att_count_to_nodes(source={}, target={}, att_name="count", through="", init=0):
  for elem in target.values():
    elem[att_name] = init
  for elem in source.values():
    through_val =  elem.get(through, "")
    if through_val:
      target_val = target.get(through_val, "")
      if target_val:
        print(through_val)
        target_val[att_name] = target_val[att_name] + 1


def add_empeding_to_nodes(source={}, cal_from="title", cuda=0):
  if cuda != -1:
      device = torch.device("cuda:" + str(cuda))
  else:
      device = torch.device("cpu")

  tokenizer = XLNetTokenizer.from_pretrained('xlnet-base-cased')
  model = XLNetModel.from_pretrained('xlnet-base-cased',
                                    output_hidden_states=True,
                                    output_attentions=True).to(device)
  for elem in source.values():
    try:
        input_ids = torch.tensor([tokenizer.encode(elem[cal_from])]).to(device)[:, :64]
        if len(input_ids[0]) < 4:
            continue
        all_hidden_states, all_attentions = model(input_ids)[-2:]
        rep = (all_hidden_states[-2][0] * all_attentions[-2][0].mean(dim=0).mean(dim=0).view(-1, 1)).sum(dim=0)
        elem['emb'] = rep.tolist()
    except Exception as e:
      print(e)

def clean_graph(graph):
  # Cleaning Graph
  print('Cleaning edge list...')
  clean_edge_list = {}
  # target_type
  for k1 in graph.edge_list:
      if k1 not in clean_edge_list:
          clean_edge_list[k1] = {}
      # source_type
      for k2 in graph.edge_list[k1]:
          if k2 not in clean_edge_list[k1]:
              clean_edge_list[k1][k2] = {}
          # relation_type
          for k3 in graph.edge_list[k1][k2]:
              if k3 not in clean_edge_list[k1][k2]:
                  clean_edge_list[k1][k2][k3] = {}

              triple_count = 0
              # target_idx
              for e1 in graph.edge_list[k1][k2][k3]:
                  edge_count = len(graph.edge_list[k1][k2][k3][e1])
                  triple_count += edge_count
                  if edge_count == 0:
                      continue
                  clean_edge_list[k1][k2][k3][e1] = {}
                  # source_idx
                  for e2 in graph.edge_list[k1][k2][k3][e1]:
                      clean_edge_list[k1][k2][k3][e1][e2] = graph.edge_list[k1][k2][k3][e1][e2]
              print(k1, k2, k3, triple_count)

  graph.edge_list = clean_edge_list
  print()
  print('Number of nodes:')
  for node_type in graph.node_forward:
      print(f'{node_type}: {len(graph.node_forward[node_type]):,}')
  print()

  del graph.node_backward


def pass_nodes_atts(graph, frm, atts=["repetition", "title", ""]):
  repetition = 0
  for rel in graph.edge_list[frm][to].keys():
      for post_idx in graph.edge_list[frm][to][rel][author_idx]:
          post_node = graph.node_backward[to][post_idx]
          repetition += post_node['repetition']
  author_node['repetition'] = repetition


graph = Graph()

############ NODES #################
# Post Nodes
post_keys = [("id", "name"), ("title", "title"), ("time", "created_utc"), ("author_id", "author"), ("subreddit_id", "subreddit_id")]
post_nodes = convertBz2FileToDictArray(file_name=posts_filename, max=max, keys_tupels=post_keys, id="name", type="post" )
# subreddit_nodes
subreddit_keys = [("id", "subreddit_id"), ("title", "subreddit")]
subreddit_nodes = convertBz2FileToDictArray(file_name=posts_filename, max=max, keys_tupels=subreddit_keys, id="subreddit_id", type="subreddit" )
# author_nodes from posts + autho
author_keys = [("id", "author"),("title", "author")]
author_nodes1 = convertBz2FileToDictArray(file_name=posts_filename, max=max, keys_tupels=author_keys, id="author" ,type="author" )
author_nodes2 = convertBz2FileToDictArray(file_name=comments_filename, max=max, keys_tupels=author_keys, id="author" ,type="author" )
author_nodes = {**author_nodes1, **author_nodes2}
# comments_nodes
comment_keys = [("id", "name"),("title", "body"), ("time", "created_utc"), ("post_id", "parent_id"), ("author_id", "author")]
comment_nodes = convertBz2FileToDictArray(file_name=comments_filename, max=max, keys_tupels=comment_keys, id="name" ,type="comment" )
# Count number of comments we have in a post
add_att_count_to_nodes(source= comment_nodes,target=post_nodes, att_name="repetition", through="post_id")


############# Embeings #############
add_empeding_to_nodes(post_nodes, cal_from="title")

############ Edges #################
# Add Post-Subreddit Edge
add_edge(graph, post_nodes, subreddit_nodes, through="subreddit_id", relation_type="PS_has_subject", time_key="time")
# Add Post-Author Edge 
add_edge(graph, post_nodes, author_nodes, through="author_id", relation_type="PA_posts", time_key="time")
# Add Comment-Author Edge
add_edge(graph, comment_nodes, author_nodes, through="author_id", relation_type="PA_posts", time_key="time")
# Add Post-Comment Edge
add_edge(graph, comment_nodes, author_nodes, through="post_id", relation_type="PC_has_comment", time_key="time")
# Add Comment-Comment Edge
add_edge(graph, nodes=comment_nodes, targets=comment_nodes, through="post_id", relation_type="CC_has_comment", time_key="time")

# TODO: implement pass_nodes_atts

print(graph.get_meta_graph())

clean_graph(graph)
# Save Graph
print('Writting graph in file:')
dill.dump(graph, open(result_folder + 'reddit_graph.pk', 'wb'))
print('Done.')
