package pl.edu.agh.ki.bd2;

import java.io.File;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Queue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;


public final class GraphDatabase {

    private final GraphDatabaseService graphDatabaseService;
    private static final String GRAPH_DIR_LOC = "./graph.db";

    public static GraphDatabase createDatabase() {
        return new GraphDatabase();
    }

    private GraphDatabase() {
        graphDatabaseService = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(new File(GRAPH_DIR_LOC))
            .setConfig(GraphDatabaseSettings.allow_upgrade, "true")
            .newGraphDatabase();
        registerShutdownHook();
    }

    private void registerShutdownHook() {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDatabaseService.shutdown();
            }
        });
    }

    public String runCypher(final String cypher) {
        try (Transaction transaction = graphDatabaseService.beginTx()) {
            final Result result = graphDatabaseService.execute(cypher);
            transaction.success();
            return result.resultAsString();
        }
    }
    
    public Node createNode(String label, Map<String, Object> properties) {
    	try (Transaction transaction = graphDatabaseService.beginTx()) {
            Node node = graphDatabaseService.createNode(Label.label(label));
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
            	node.setProperty(entry.getKey(), entry.getValue());
            }
            transaction.success();
            return node;
        }
    }
    
    public Relationship createRelationship(Node node1, Node node2, String relationshipName) {
    	try (Transaction transaction = graphDatabaseService.beginTx()) {
    		Relationship r = node1.createRelationshipTo(node2, RelationshipType.withName(relationshipName));
            transaction.success();
            return r;
        }
    }
    
    public Node createNodeA(String name) {
    	HashMap<String, Object> properties = new HashMap<String, Object>();
    	properties.put("name", name);
    	return createNode("A", properties);
    }
    
    public Node createNodeB(String name) {
    	HashMap<String, Object> properties = new HashMap<String, Object>();
    	properties.put("name", name);
    	return createNode("B", properties);
    }
    
    public Node createNodeC(String name) {
    	HashMap<String, Object> properties = new HashMap<String, Object>();
    	properties.put("name", name);
    	return createNode("C", properties);
    }
    
    public Relationship createRelationshipX(Node node1, Node node2) {
    	return createRelationship(node1, node2, "X");
    }
    
    public Relationship createRelationshipY(Node node1, Node node2) {
    	return createRelationship(node1, node2, "Y");
    }
    
    public void populate() {
    	Node a1 = createNodeA("1");
    	Node a2 = createNodeA("2");
    	Node a3 = createNodeA("3");
    	Node a4 = createNodeA("4");
    	Node a5 = createNodeA("5");
    	
    	Node b1 = createNodeB("11");
    	Node b2 = createNodeB("22");
    	Node b3 = createNodeB("33");
    	Node b4 = createNodeB("44");
    	Node b5 = createNodeB("55");
    	
    	Node c1 = createNodeC("111");
    	Node c2 = createNodeC("222");
    	Node c3 = createNodeC("333");
    	Node c4 = createNodeC("444");
    	Node c5 = createNodeC("555");
    	
    	createRelationshipX(a1, b3);
    	createRelationshipX(a1, b4);
    	createRelationshipX(a1, b1);
    	createRelationshipX(a2, b1);
    	createRelationshipX(a2, b3);
    	createRelationshipX(a3, b2);
    	createRelationshipX(a3, b3);
    	createRelationshipX(a4, b3);
    	createRelationshipX(a4, b2);
    	createRelationshipX(a5, b5);
    	createRelationshipX(a5, b3);
    	createRelationshipX(a5, b1);
    	
    	createRelationshipY(b1, c1);
    	createRelationshipY(b1, c4);
    	createRelationshipY(b1, c3);
    	createRelationshipY(b2, c5);
    	createRelationshipY(b2, c2);
    	createRelationshipY(b3, c3);
    	createRelationshipY(b3, c2);
    	createRelationshipY(b3, c1);
    	createRelationshipY(b4, c5);
    	createRelationshipY(b5, c4);
    	createRelationshipY(b5, c1);
    }
    
    public void displayRelationships(String nodeLabel, String nodeName) {
    	try (Transaction transaction = graphDatabaseService.beginTx()) {
    		Node node = graphDatabaseService.findNode(Label.label(nodeLabel), "name", nodeName);
    		for (Relationship r : node.getRelationships()) {
    			Node start = r.getStartNode();
    			Node end = r.getEndNode();
    			System.out.println("(" + start.getProperty("name")+") -[" + 
    					r.getType().name() + "]-> (" + end.getProperty("name") + ")");
    		}
            transaction.success();
        }
    }
    
    private void printRoute(Map<String, String> parents, String nodeToBeFound) {
    	List<String> shortestPath = new ArrayList<>();
        String node = nodeToBeFound;
        while(node != null) {
            shortestPath.add(node);
            node = parents.get(node);
        }
        Collections.reverse(shortestPath);
        for (String s : shortestPath) {
        	System.out.println(s);
        }
    }
    
    public boolean findRoute(String label1, String name1, String label2, String name2) {
    	try (Transaction transaction = graphDatabaseService.beginTx()) {
    		Node startNode = graphDatabaseService.findNode(Label.label(label1), "name", name1);
    		Queue<Node> queue = new LinkedList<Node>();
    		Set<Long> visited = new HashSet<Long>();
    		Map<String, String> parentNodes = new HashMap<String, String>();
    		
            queue.add(startNode);
            while (!queue.isEmpty()) {
                Node nextNode = queue.poll();
                if (nextNode.hasLabel(Label.label(label2)) && nextNode.getProperty("name") == name2) {
                	printRoute(parentNodes, name2);
                	transaction.success();
                	return true;
                }
                
                visited.add(nextNode.getId());
                for (Relationship r : nextNode.getRelationships(Direction.OUTGOING)) {
                	if (!visited.contains(r.getEndNode().getId())) {
                		queue.add(r.getEndNode());
                		parentNodes.put((String)nextNode.getProperty("name"), 
                				(String) r.getEndNode().getProperty("name"));
                	}
                }
            }
            System.out.println("Couldn't find a path");
            return false;
    	}
    }
}
