import json
import kg2_util
import jsonlines
import argparse

def make_arg_parser():
	arg_parser = argparse.ArgumentParser(description='build-kg2: builds the KG2 knowledge graph for the RTX system')
	arg_parser.add_argument('inputEdgesFile', type=str)
	arg_parser.add_argument('outputPathsFile', type=str)
	return arg_parser

def get_sample_edges():
	# format:
	# graph = {"subject 1": [(predicate a, object a), (predicate b, object b), (predicate c, object c)], "subject 2": [(predicate d, object d)]}

	predicates = ["&&&", "###", "$$$"]

	graph = {"a": [(predicates[0], "b"), (predicates[1], "b"), (predicates[1], "c"), (predicates[2], "d")],
			 "b": [(predicates[1], "c"), (predicates[2], "d"), (predicates[1], "e")],
			 "c": [(predicates[1], "f"), (predicates[1], "g"), (predicates[1], "j")],
			 "d": [],
			 "e": [(predicates[1], "j")],
			 "f": [],
			 "g": [],
			 "h": [(predicates[2], "i")],
			 "i": [(predicates[2], "k")],
			 "j": [(predicates[1], "a")],
			 "k": []}

	return graph

def get_edges(edges_filename):
	formatted_edges = dict()

	edges_read_jsonlines_info = kg2_util.start_read_jsonlines(edges_filename)
	input_edges = edges_read_jsonlines_info[0]

	for input_edge in input_edges:
		subject_id = input_edge["subject"]
		object_id = input_edge["object"]
		predicate = input_edge["source_predicate"]

		if subject_id not in formatted_edges:
			formatted_edges[subject_id] = list()
		formatted_edges[subject_id].append((predicate, object_id))

	kg2_util.end_read_jsonlines(edges_read_jsonlines_info)

	return formatted_edges

def find_all_paths(graph, output_paths):
	for subject_id in graph:
		subject_paths = find_all_subject_paths(graph, subject_id, [], [], output_paths)

		output_paths.write({subject_id: subject_paths})

def find_all_subject_paths(graph, subject_id, paths, current_path, output_paths):
	for (predicate, object_id) in graph.get(subject_id, []):
		edge = (subject_id, predicate, object_id)

		# Avoid cycles
		if edge in current_path:
			continue

		longer_path = current_path + [edge]

		output_paths.write(longer_path)

		find_all_subject_paths(graph, object_id, paths, longer_path)

	return paths

def all_paths_between_two_nodes(paths):
	paths_between_two_nodes = dict()

	for subject_id in paths:
		for path in paths[subject_id]:
			first_subject = path[0][0]
			last_object = path[-1][2]

			two_nodes = (first_subject, last_object)
			if two_nodes not in paths_between_two_nodes:
				paths_between_two_nodes[two_nodes] = list()
			paths_between_two_nodes[two_nodes].append(path)

	return paths_between_two_nodes


def print_path(path):
	arrow = ' ---> '
	path_str = str()
	for (subject_id, predicate, object_id) in path:
		if path_str == "":
			path_str += subject_id

		path_str += arrow + predicate + arrow + object_id
		# path_str += arrow + object_id
	print(path_str)

def print_paths(paths):
	for subject_id in paths:
		for path in paths[subject_id]:
			print_path(path)

if __name__ == '__main__':
	args = make_arg_parser().parse_args()
	input_edges_file_name = args.inputEdgesFile
	output_paths_file_name = args.outputPathsFile

	graph = get_edges(input_edges_file_name)

	output_paths_writer = kg2_util.create_single_jsonlines()
	output_paths = output_paths_writer[0]

	paths = find_all_paths(graph, output_paths)

	kg2_util.close_single_jsonlines(output_paths_writer, output_paths_file_name)

	# paths_between_two_nodes = all_paths_between_two_nodes(paths)
	# for two_nodes in paths_between_two_nodes:
	# 	print(two_nodes)
	# 	for path in paths_between_two_nodes[two_nodes]:
	# 		print_path(path)

	# 	print("--------\n\n")