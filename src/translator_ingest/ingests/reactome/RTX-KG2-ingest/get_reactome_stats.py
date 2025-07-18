#!/usr/bin/env python3

'''Provides a JSON overview report of a Reactome, JSONLines knowledge graph.

   Usage: get_reactome_stats.py <inputNodesFile.jsonl> <inputEdgesFile.jsonl> <outputFile.json>
'''

__author__ = 'Erica Wood'
__copyright__ = 'Oregon State University'
__credits__ = ['Stephen Ramsey', 'Erica Wood']
__license__ = 'MIT'
__version__ = '0.1.0'
__maintainer__ = ''
__email__ = ''
__status__ = 'Prototype'


import argparse
import kg2_util
import jsonlines
import datetime

CORE_PREDICATE_KEY = 'core_predicate'
QUALIFIED_PREDICATE_KEY = 'qualified_predicate'
OBJECT_DIRECTION_KEY = 'object_direction'

PREDICATE_MAP = {'REACT:has_element': {CORE_PREDICATE_KEY: 'biolink:has_part'},
                 'REACT:has_event': {CORE_PREDICATE_KEY: 'biolink:has_participant'},
                 'REACT:has_input': {CORE_PREDICATE_KEY: 'biolink:has_input'},
                 'REACT:has_member': {CORE_PREDICATE_KEY: 'biolink:has_part'},
                 'REACT:has_output': {CORE_PREDICATE_KEY: 'biolink:has_output'},
                 'REACT:in_species': {CORE_PREDICATE_KEY: 'biolink:occurs_in'},
                 'REACT:is_requirement_for': {CORE_PREDICATE_KEY: 'biolink:regulates'},
                 'REACT:linked_to_disease': {CORE_PREDICATE_KEY: 'biolink:related_to'},
                 'REACT:negatively_regulates': {CORE_PREDICATE_KEY: 'biolink:regulates', QUALIFIED_PREDICATE_KEY: 'biolink:causes', OBJECT_DIRECTION_KEY: 'downregulated'},
                 'REACT:negatively_regulates_gene_expression': {CORE_PREDICATE_KEY: 'biolink:regulates', QUALIFIED_PREDICATE_KEY: 'biolink:causes', OBJECT_DIRECTION_KEY: 'downregulated'},
                 'REACT:positively_regulates': {CORE_PREDICATE_KEY: 'biolink:regulates', QUALIFIED_PREDICATE_KEY: 'biolink:causes', OBJECT_DIRECTION_KEY: 'upregulated'},
                 'REACT:positive_regulates_gene_expression': {CORE_PREDICATE_KEY: 'biolink:regulates', QUALIFIED_PREDICATE_KEY: 'biolink:causes', OBJECT_DIRECTION_KEY: 'upregulated'},
                 'REACT:related_to': {CORE_PREDICATE_KEY: 'biolink:related_to'}}

CATEGORY_KEY = 'category'
REACTOME_CATEGORY_KEY = 'reactome_category'
REACTOME_REFERENCE_CLASS_KEY = 'reactome_reference_class'
CATEGORY_STORE_KEY = 'category_store'

REACTOME_PREFIX = kg2_util.CURIE_PREFIX_REACTOME

def make_arg_parser():
    arg_parser = argparse.ArgumentParser(description='build-kg2: builds the KG2 knowledge graph for the RTX system')
    arg_parser.add_argument('inputNodesFile', type=str)
    arg_parser.add_argument('inputEdgesFile', type=str)
    arg_parser.add_argument('outputFile', type=str)
    return arg_parser


def get_prefix_from_curie_id(curie_id: str):
    assert ':' in curie_id
    
    return curie_id.split(':')[0]


def get_node_stats(nodes_file_name):
    nodes_read_jsonlines_info = kg2_util.start_read_jsonlines(nodes_file_name)
    nodes = nodes_read_jsonlines_info[0]

    category_report = dict()
    category_store_report = dict()
    reactome_category_report = dict()

    # Non-global keys
    id_key = 'id'

    # Initialize our output data
    node_count = 0
    simple_nodes = dict()

    for node in nodes:
        node_count += 1

        category = node[CATEGORY_KEY]
        node_id = node[id_key]
        reactome_category = node[REACTOME_CATEGORY_KEY]
        reactome_reference_class = node[REACTOME_REFERENCE_CLASS_KEY]

        category_store = str((category, reactome_category))
        if reactome_reference_class is not None:
            category_store = str((category, reactome_category, reactome_reference_class))

        if category_store not in category_store_report:
            category_store_report[category_store] = 0
        category_store_report[category_store] += 1

        if reactome_category not in reactome_category_report:
            reactome_category_report[reactome_category] = 0
        reactome_category_report[reactome_category] += 1

        if category not in category_report:
            category_report[category] = 0
        category_report[category] += 1

        simple_nodes[node_id] = {CATEGORY_KEY: category, REACTOME_CATEGORY_KEY: reactome_category, CATEGORY_STORE_KEY: category_store}

    # Close our reader since we have finished
    kg2_util.end_read_jsonlines(nodes_read_jsonlines_info)

    node_report = {'_number_of_nodes': node_count,
                   'number_of_nodes_by_category': category_store_report,
                   'number_of_nodes_by_reactome_category': reactome_category_report,
                   'number_of_nodes_by_category_conglomerate': category_store_report}

    return nodes_report, simple_nodes


def get_edge_stats(edges_file_name, nodes):
    edges_read_jsonlines_info = kg2_util.start_read_jsonlines(edges_file_name)
    edges = edges_read_jsonlines_info[0]

    # Edge Access Keys
    relation_key = 'relation'
    subject_key = 'subject'
    object_key = 'object'

    # Initialize our output data
    edge_count = 0
    relations_report = dict()
    core_predicates_report = dict()
    predicates_store_report = dict()
    combos_report = dict()

    for edge in edges:
        edge_count += 1

        relation = edge[relation_key]
        subject_curie = edge[subject_key]
        subject_prefix = get_prefix_from_curie_id(subject_curie)
        object_curie = edge[object_key]
        object_prefix = get_prefix_from_curie_id(object_curie)
        predicate = PREDICATE_MAP[relation]
        core_predicate = predicate[CORE_PREDICATE_KEY]
        qualified_predicate = predicate.get(QUALIFIED_PREDICATE_KEY, None)
        object_direction = predicate.get(OBJECT_DIRECTION_KEY, None)

        if qualified_predicate is not None or object_direction is not None:
            predicate_store = str((core_predicate, qualified_predicate, object_direction))
        else:
            predicate_store = core_predicate

        if subject_prefix == REACTOME_PREFIX:
            subject_node = nodes[subject_curie]
            subject_node_category_info = subject_node[CATEGORY_KEY]
            subject_node_reactome_category_info = subject_node[REACTOME_CATEGORY_KEY]
            subject_node_category_store_info = subject_node[CATEGORY_STORE_KEY]
        else:
            subject_node_category_info = subject_prefix
            subject_node_reactome_category_info = subject_prefix
            subject_node_category_store_info = subject_prefix

        if object_prefix == REACTOME_PREFIX:
            object_node = nodes[object_curie]
            object_node_category_info = object_node[CATEGORY_KEY]
            object_node_reactome_category_info = object_node[REACTOME_CATEGORY_KEY]
            object_node_category_store_info = object_node[CATEGORY_STORE_KEY]
        else:
            object_node_category_info = object_prefix
            object_node_reactome_category_info = object_prefix
            object_node_category_store_info = object_prefix

        edge_label_types = {'relation': relation,
                            'predicate': core_predicate,
                            'qualifier_predicate': predicate_store}
        node_info_types = {'category': (subject_node_category_info, object_node_category_info),
                           'reactome_category': (subject_node_reactome_category_info, object_node_reactome_category_info),
                           'category_conglomerate': (subject_node_category_store_info, object_node_category_store_info)}

        for edge_label_type in edge_label_types:
            for node_info_type in node_info_types:
                key = 'number_of_' + edge_label_type + '_' + node_info_type + '_combos'
                edge_label = edge_label_types[edge_label_type]
                subject_info = node_info_types[node_info_type][0]
                object_info = node_info_types[node_info_type][1]
                combo = subject_info + '---' + edge_label + '---' + object_info

                if key not in combos_report:
                    combos_report[key] = dict()

                if combo not in combos_report[key]:
                    combos_report[key][combo] = 0
                combos_report[key][combo] += 1

        if relation not in relations_report:
            relations_report[relation] = 0
        relations_report[relation] += 1

        if core_predicate not in core_predicates_report:
            core_predicates_report[core_predicate] = 0
        core_predicates_report[core_predicate] += 1

        if predicate_store not in predicates_store_report:
            predicates_store_report[predicate_store] = 0
        predicates_store_report[predicate_store] += 1

    # Close our reader since we have finished
    kg2_util.end_read_jsonlines(edges_read_jsonlines_info)

    edge_report = combos_report()
    
    edges_report['number_of_edges_by_relation'] = relations_report
    edges_report['number_of_edges_by_core_predicates'] = core_predicates_report
    edges_report['number_of_edges_by_qualified_predicates'] = predicates_store_report

    return edges_report


if __name__ == '__main__':
    args = make_arg_parser().parse_args()
    input_nodes_file_name = args.inputNodesFile
    input_edges_file_name = args.inputEdgesFile

    stats = {'_report_datetime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    # Get stats from the edges first (since we need the set of nodes on edges), then nodes
    nodes_report, simple_nodes = get_node_stats(input_nodes_file_name)
    edges_report = get_edge_stats(input_edges_file_name, simple_nodes)

    # Add the output of get_edge_stats() and get_node_stats() to the return dictionary
    stats.update(edges_report)
    stats.update(nodes_report)

    # Save our output dictionary to the output file
    kg2_util.save_json(stats, args.outputFile, True)
