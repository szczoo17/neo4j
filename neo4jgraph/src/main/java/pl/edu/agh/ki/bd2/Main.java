package pl.edu.agh.ki.bd2;

import java.io.IOException;

public class Main {

	public static void main(String[] args) throws IOException {
        GraphDatabase db = GraphDatabase.createDatabase();
        db.displayRelationships("B", "33");
    }
}
