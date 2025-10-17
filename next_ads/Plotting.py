import plotly.graph_objects as go
import networkx as nx
import numpy as np
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class DirectedGraphPlotter:
    '''
    Create and save an directed graph as html file.
    PySpark DataFrame `df` must contain columns:
        `node`, `next_node`, `node_weight`, `edge_weight`
    '''

    def __init__(
            self,
            df: DataFrame,
            min_edge_weight: float = None,
            min_node_weight: float = None,
            colorscale: str = 'tempo'
            ):
        self.df = df
        self.min_edge_weight = min_edge_weight
        self.min_node_weight = min_node_weight
        self.colorscale = colorscale

    def _extract_columns(self):
        self.df = self.df.select('node', 'next_node',
                                 'node_weight', 'edge_weight')

    def _filter_edges(self):
        self.df = self.df.where(F.col('edge_weight') > self.min_edge_weight)

    def _filter_nodes(self):
        self.df = self.df.where(F.col('node_weight') > self.min_node_weight)

    def _format_graph_connections(self, node):
        '''Sort connections (descending) and format for hover'''
        if node not in self.node_connections:
            return "No connections"

        sorted_conns = sorted(self.node_connections[node],
                              key=lambda x: x[1], reverse=True)

        # Format as "• Node (weight)"
        formatted_conns = [f"• {neighbor} ({edge_weight:.3f})"
                           for neighbor, edge_weight in sorted_conns]
        return '<br>'.join(formatted_conns)

    def _build_graph(self):
        # Preprocess dataframe for graph creation
        self._extract_columns()
        if self.min_edge_weight:
            self._filter_edges()
        if self.min_node_weight:
            self._filter_nodes()
        self.graph_df = self.df.toPandas()

        # Create directed graph
        self.G = nx.DiGraph()
        for _, row in self.graph_df.iterrows():
            self.G.add_edge(row['node'], row['next_node'],
                            weight=row['edge_weight'])

        self.node_connections = {}
        for _, row in self.graph_df.iterrows():
            node = row['node']
            next_node = row['next_node']
            edge_weight = row['edge_weight']

            if node not in self.node_connections:
                self.node_connections[node] = []

            self.node_connections[node].append((next_node, edge_weight))

        self.node_weights = {}
        for _, row in self.graph_df.iterrows():
            self.node_weights[row['node']] = (
                self.node_weights.get(row['node'], 0)
                + row['node_weight']
            )
            self.node_weights[row['next_node']] = (
                self.node_weights.get(row['next_node'], 0)
                + row['node_weight']
            )

        self.pos = nx.spring_layout(self.G)

        self.edge_traces = []
        for edge in self.G.edges(data=True):
            x0, y0 = self.pos[edge[0]]
            x1, y1 = self.pos[edge[1]]
            self.edge_traces.append(go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode='lines',
                line=dict(width=edge[2]['weight']*5, color='gray'),
                hoverinfo='none'
            ))

        # Scale node sizes
        self.sizes = np.array(
            [self.node_weights.get(node, 1) for node in self.G.nodes()])
        self.log_sizes = np.log10(self.sizes + 1)  # +1 to handle any zeros
        self.scaled_sizes = (
            10 + (self.log_sizes - self.log_sizes.min())
            / (self.log_sizes.max() - self.log_sizes.min())
            * 90)

        # Create node trace
        self.node_trace = go.Scatter(
            x=[self.pos[node][0] for node in self.G.nodes()],
            y=[self.pos[node][1] for node in self.G.nodes()],
            mode='markers+text',
            marker=dict(
                size=self.scaled_sizes.tolist(),
                color=[self.node_weights.get(node, 1)
                       for node in self.G.nodes()],
                colorscale=self.colorscale,
                showscale=False,
                colorbar=dict(title='Node Weight'),
                line=dict(width=2, color='black')
            ),
            text=list(self.G.nodes()),
            textposition='top center',
            hovertemplate='<b>%{text}</b><br>' +
                          'Node weight: %{customdata[0]}<br>' +
                          'Connected nodes:<br>%{customdata[1]}<br>' +
                          '<extra></extra>',
            customdata=[[f"{self.node_weights.get(node, 1):,}",
                        self._format_graph_connections(node)]
                        for node in self.G.nodes()],
            name='Nodes'
        )

    def create_figure(self, template: str = 'simple_white'):

        self._build_graph()

        self.fig = go.Figure(data=self.edge_traces + [self.node_trace])
        self.fig.update_layout(
            template=template,
            showlegend=False,
            xaxis=dict(showgrid=False, zeroline=False,
                       showticklabels=False, visible=False),
            yaxis=dict(showgrid=False, zeroline=False,
                       showticklabels=False, visible=False)
        )
