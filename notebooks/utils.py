import networkx as nx
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from gridsample.utils import create_ids


def build_graph_from_gdf_with_distance_threshold(
    gdf,
    distance_threshold=1000,
    n_jobs=-1,
) -> nx.Graph:
    """
    Build a graph from a GeoDataFrame where the nodes are the index of the GeoDataFrame
    and the edges are the distance between the geometries.

    Parameters
    ----------
    gdf : GeoDataFrame
        The GeoDataFrame to build the graph from. Must be in a projected coordinate system.
    distance_threshold : float
        The maximum distance (meters) to build edges between geometries.
    n_jobs : int
        The number of jobs to run in parallel. -1 means using all processors.

    Returns
    -------
    nx.Graph
        The graph with the distances as the edge attribute.
    """

    if gdf.crs == "EPSG:4326":
        raise ValueError("The GeoDataFrame must be in a projected coordinate system.")

    gdf_temp = gdf.copy()
    gdf_temp["row_number"] = np.arange(len(gdf_temp))

    # determine which edges should be added
    def build_edges_to_nearby_geometries(i, geom1, gdf, distance_threshold):
        buffered_geom = geom1.buffer(distance_threshold)
        gdf_intersecting_subset = gdf[gdf.intersects(buffered_geom)]
        edges = []
        for j, geom2 in zip(
            gdf_intersecting_subset["row_number"], gdf_intersecting_subset["geometry"]
        ):
            if i < j:
                distance = geom1.distance(geom2)
                edges.append((gdf_temp.index[i], gdf_temp.index[j], distance))
        return edges

    # apply the function in parallel
    list_of_lists_of_edges = Parallel(n_jobs=n_jobs)(
        delayed(build_edges_to_nearby_geometries)(
            i, geom1, gdf_temp, distance_threshold
        )
        for i, geom1 in zip(gdf_temp["row_number"], gdf_temp["geometry"])
    )

    # flatten the list of lists of edges
    edges = [edge for edges in list_of_lists_of_edges for edge in edges]

    # Build the graph
    G = nx.Graph()
    G.add_nodes_from(gdf_temp.index)
    G.add_weighted_edges_from(edges)

    print(
        f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges."
    )
    return G


def get_connected_components_by_distance_threshold(
    G, distance_threshold=None, cluster_id_col_name="cluster_id", cluster_id_prefix="CLUSTER_"
) -> tuple[pd.DataFrame, nx.Graph]:
    """
    Get the connected components of a graph by a distance threshold. The connected components are
    the nodes that are within the distance threshold of each other.

    Parameters
    ----------
    G : nx.Graph
        The graph to get the connected components from. The graph must have "weight" as the edge attribute.
    distance_threshold : float, optional
        The distance threshold to use to get the connected components.

    Returns
    -------
    cluster_labels_df : pd.DataFrame
        A DataFrame that maps each node (index) to its cluster label.
    G_filtered_with_cluster_labels : nx.Graph
        The graph with edges filtered by the distance threshold with the cluster labels as node attributes.
    """

    def filter_edges_by_weight(G, max_weight):
        """Helper function - Filter a graph by a maximum weight threshold."""

        # this looping method might be slow. Fix later if need be.
        G_filtered = G.copy()
        for edge in G.edges(data=True):
            if edge[2]["weight"] > max_weight:
                G_filtered.remove_edge(edge[0], edge[1])
        return G_filtered

    # filter to only the edges that are within the distance threshold and get the connected components
    if distance_threshold:
        G_filtered = filter_edges_by_weight(G, max_weight=distance_threshold)
    else:
        G_filtered = G

    # print(len(G.edges), "edges filtered to", len(G_filtered.edges), "by distance threshold", distance_threshold)
    list_of_sets_of_connected_nodes = list(nx.connected_components(G_filtered))

    # create parcel ids
    parcel_ids = create_ids(len(list_of_sets_of_connected_nodes), prefix=cluster_id_prefix)

    # create a dictionary that maps each node to its parcel_id.
    node_to_parcel_id = {}
    for parcel_id, connected_nodes in zip(parcel_ids, list_of_sets_of_connected_nodes):
        for node in connected_nodes:
            node_to_parcel_id[node] = parcel_id

    # create a dataframe that maps each node (as index) to its parcel_id
    cluster_labels_df = pd.DataFrame(
        {cluster_id_col_name: node_to_parcel_id.values()},
        index=list(node_to_parcel_id.keys()),
    )
    cluster_labels_df.sort_index(inplace=True)

    # add the parcel_id attribute to the nodes
    G_filtered_with_parcel_id = G_filtered.copy()
    for node in G_filtered_with_parcel_id.nodes:
        G_filtered_with_parcel_id.nodes[node][cluster_id_col_name] = node_to_parcel_id[
            node
        ]

    return cluster_labels_df, G_filtered_with_parcel_id


def add_parcel_id_to_nodes(G, node_to_parcel_id, cluster_id_col_name="cluster_id"):
    G_with_parcel_id = G.copy()
    for node in G_with_parcel_id.nodes:
        G_with_parcel_id.nodes[node][cluster_id_col_name] = node_to_parcel_id[
            node
        ]
    return G_with_parcel_id


def get_intra_parcel_distance_stats(G_filtered, parcel_ids, parcel_id_col="parcel_id"):
    """
    Get the inter-khasra distance stats within each parcel.

    Parameters
    ----------
    G_filtered : nx.Graph
        The graph with the edges filtered by the distance threshold.
    parcel_ids : list
        The list of parcel_ids to get the distances for.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the parcel_id and corresponding distance stats.
    """

    # helper function to get the edge weights of a parcel_id
    def get_edge_weights_by_parcel_id(G_filtered, parcel_id):
        selected_nodes = {
            n for n, d in G_filtered.nodes(data=True) if d.get(parcel_id_col) == parcel_id
        }
        subgraph = G_filtered.subgraph(selected_nodes)
        edge_weights = [d["weight"] for _, _, d in subgraph.edges(data=True)]
        return edge_weights

    distances_list = []
    for parcel_id in parcel_ids:
        distances = get_edge_weights_by_parcel_id(G_filtered, parcel_id)
        if len(distances) == 0:
            avg_distance = 0
            min_distance = 0
            percentile_25th_distance = 0
            percentile_50th_distance = 0
            percentile_75th_distace = 0
            max_distance = 0
        else:
            # avg
            avg_distance = np.mean(distances)
            # min 25% quartile, median, 75% quantile, max
            min_distance = np.min(distances)
            percentile_25th_distance = np.percentile(distances, 20)
            percentile_50th_distance = np.percentile(distances, 50)
            percentile_75th_distace = np.percentile(distances, 75)
            max_distance = np.max(distances)

        distances_list.append({
            parcel_id_col: parcel_id,
            "Inter-Khasra Distance Average (m)": avg_distance,
            "Inter-Khasra Distance Min (m)": min_distance,
            "Inter-Khasra Distance 25th Percentile (m)": percentile_25th_distance,
            "Inter-Khasra Distance Median (m)": percentile_50th_distance,
            "Inter-Khasra Distance 75th Percentile (m)": percentile_75th_distace,
            "Inter-Khasra Distance Max (m)": max_distance,
            "raw_distances": distances,
        })
    
    return pd.DataFrame(distances_list).round(2)

def get_closest_parcels(gdf, parcel_id_col="parcel_id"):
    min_distances = []
    closest_ids = []
    for i in range(len(gdf)):
        geom = gdf.iloc[i].geometry
        other_geoms = gdf.drop(gdf.index[i])
        distances = other_geoms.geometry.apply(lambda x: geom.distance(x))
        min_distance = distances.min().round(2)
        closest_id = other_geoms.loc[distances.idxmin()][parcel_id_col]
        min_distances.append(min_distance)
        closest_ids.append(closest_id)
    return min_distances, closest_ids