package demo;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Analysis {

    public static void main(String... args) throws IOException {
        new Analysis().go();
    }

    private GraphDatabaseService db;

    void go() throws IOException {

        File pathFile = new File("paths.log");
        BufferedWriter pathsWriter = new BufferedWriter(new FileWriter(pathFile, false));

        File genomeDivFile = new File("genomeDivs.log");
        BufferedWriter genomeDivWriter = new BufferedWriter(new FileWriter(genomeDivFile, false));

        for (int c = 0; c < 3; c++) {
            for (int t = 0; t < 20; t++) {
                File dbFile = new File("graph-c" + c + "-t" + t + ".db");
                db = new GraphDatabaseFactory().newEmbeddedDatabase(dbFile);
                System.out.printf("\n\nconfig: %d\ntrial: %d\n", c, t);

                // genomes as properties
                db.execute(saveTriplesToEachCell);

                // paths
                Result pathsResult = db.execute(getPathsForEachConfig);
                pathsWriter.write(String.format("\n\nconfig: %d\ntrial: %d\n", c, t));
                pathsWriter.write(pathsResult.resultAsString());
                pathsWriter.flush();

                // genome diversity
//                Result genomeDivResult = db.execute(genomeDiversity);
//                genomeDivWriter.write(String.format("\n\nconfig: %d\ntrial: %d\n", c, t));
//                genomeDivWriter.write(genomeDivResult.resultAsString());
//                genomeDivWriter.flush();

                db.shutdown();
            }
        }
        pathsWriter.close();
        genomeDivWriter.close();
    }

    private String genomeDiversity =
            "// get distinct genome count per iteration\n" +
                    "unwind range(0,99) as t\n" +
                    "match (c:CELL)\n" +
                    "with t, collect(c) as cells\n" +
                    "with t, filter(c in cells WHERE c.start <= t and (not (c:INACTIVE) or c.inactiveAt > t)) as cells\n" +
                    "unwind cells as c\n" +
                    "return t, count(distinct c.genome) as nGenomes\n" +
                    "order by t";

    private String saveTriplesToEachCell =
            "match (c)-[:CONTAINS]->(ch:CHROMOSOME)-[:CONTAINS]->(g:GENE)\n" +
                    "where (c:CELL) or (c:CELL_COPY)\n" +
                    "with c, collect(g) as genes\n" +
                    "with c,\n" +
                    "\tsize(filter(x in genes where (x:APOPT_GENE))) as a,\n" +
                    "\tsize(filter(x in genes where (x:DIV_GENE))) as d,\n" +
                    "\tsize(filter(x in genes where (x:SEG_GENE))) as s\n" +
                    "set c += {apt: a, div: d, seg: s, genome: [a,d,s]}";
    
    private String getPathsForEachConfig =
            "// get all distinct paths for all aneuploid cell configs\n" +
                    "\n" +
                    "// get different living  cell configurations\n" +
                    "match (c:CELL)\n" +
                    "where not (c:INACTIVE) and exists(c.genome) and not c.genome=[2,2,2]\n" +
                    "with distinct c.genome as gen\n" +
                    "\n" +
                    "// get all distinct paths for chosen genome\n" +
                    "match (c:CELL) where (not (c:INACTIVE)) and c.genome=gen\n" +
                    "with gen, c match p = (c)-[:FROM|WAS*]->(o) where ((o:CELL) or (o:CELL_COPY)) and not (o)-[:FROM|WAS]->() and o.start = 0\n" +
                    "with gen, p, extract(x in nodes(p) | x.genome) as fullGenomeSequence\n" +
                    "with gen, p, reduce(out = [], x in fullGenomeSequence | case last(out) when x then out else out + [x] end) as path\n" +
                    "return distinct\n" +
                    "\tgen,\n" +
                    "\tcount(p) as cells,\n" +
                    "    path,\n" +
                    "    size(path) as numChanges\n" +
                    "order by gen, cells desc";

}
