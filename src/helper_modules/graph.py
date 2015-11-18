from collections import OrderedDict

# Graph implementation adapted from Graphs in Python (http://www.python-course.eu/graphs_python.php) to suit the chosen design f
'''
    Class implementing the behaviors, attributes and functionalities of a graph needed for a
    hashtag graph
    
    Using OrderedDict to hold order of insertion of hashtags into graph.
    
'''

# Graph implemented with a dictionary
class Graph(object):

    '''
        initializes a graph object
    '''
    
    def __init__(self, graph_dict = OrderedDict()):
        self.graph_dict = graph_dict

    '''
        Getter: Function that returns all vertices currently  of a graph
        :rtype List(str)
    '''
    def get_vertices(self):
        return list(self.graph_dict.keys())

    '''
        Getter: Returns edges of a graph
        :rtype List(set)
    '''
    
    def get_edges(self):
        return self.generate_graph_edges()

    '''
        Setter: Adds a vertex to a graph
        
        If vertex exists in graph, nothing is added, 
        otherwise adds vertex with an empty list
        to collect it's adjacent neighbors. 
    '''
    def add_vertex(self, vertex):
        if (vertex not in self.graph_dict):
            self.graph_dict[vertex] = []

    
    '''
        Setter: Adds an edge to a graph
        
        Edge is assumed as a set of iterable; tuple, list or set
    '''       
    
    def add_edge(self, edge):
        (v1, v2) = tuple(edge)
        if (v1 in self.graph_dict):
            if (v2 not in self.graph_dict[v1]):
                self.graph_dict[v1].append(v2)
        else:
            self.graph_dict[v1] = [v2]

    
    
    '''
        Function that removes edge between two vertices.
        Edge is a set/tuple of vertices
    '''
    def remove_edge(self, edge):
        (v1, v2) = tuple(edge)
        if (v1 in self.graph_dict):
            if (v2 in self.graph_dict[v1]): # For readability, could use one if cond1 and cond2
                self.graph_dict[v1].remove(v2)
                
                
    
    '''
        Function that calculates the degree of a single vertex.
        
        Degree being number of edges leading out of a vertex to 
        adject vertices. In general cases a vertex can have a cycle,
        but for simplicity in this challenge we avoid cycles. But if 
        cycles were allowed, then degree of a vertex would be sum of adjacent
        vertices plus occurence of vertex in it's own adject neighbor list
    '''
    def vertex_degree(self, vertex):
        neighbors =  self.graph_dict[vertex]
        degree = len(neighbors) + neighbors.count(vertex)
        return degree
    
    
    '''
        Calculate Complete Graph Average Degrees
    '''
    def get_graph_average_degrees(self):
        running_total = 0
        for vertex in self.graph_dict:
            running_total += self.vertex_degree(vertex)
        return running_total / float(len(self.graph_dict))
    
    '''
        Helper Function: Generate the edges of the graph.
        
        Edges are internally represented as set two vertices 
    '''
    def generate_graph_edges(self):
        edges = []
        for vertex in self.graph_dict:
            for neighbour in self.graph_dict[vertex]:
                if ({neighbour, vertex} not in edges):
                    edges.append({vertex, neighbour})
        return edges

    '''
        Function that finds a path between two vertices in graph
    '''
    def find_path(self, st, en, path=[]):
        graph = self.graph_dict
        path = path + [st]
        if (st == en):
            return path
        if (st not in graph):
            return None
        for vertex in graph[st]:
            if (vertex not in path):
                extended_path = self.find_path(vertex, 
                                               en, 
                                               path)
                if (extended_path): 
                    return extended_path
        return None
   

    '''
        Overwritten function: To have a representation of graph when printed.
    '''
    def __str__(self):
        result = "vertices: "
        for v in self.graph_dict:
            result += str(v) + " "
        result += "\nedges: "
        for e in self.generate_graph_edges():
            result += str(e) + " "
        return result
