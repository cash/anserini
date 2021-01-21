/*
 * Anserini: A Lucene toolkit for replicable information retrieval research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.anserini.ann;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.ann.fw.FakeWordsEncoderAnalyzer;
import io.anserini.ann.lexlsh.LexicalLshAnalyzer;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

public class ApproximateNearestNeighborSearchCollection {
  private static final String FW = "fw";
  private static final String LEXLSH = "lexlsh";

  public static final class Args {
    @Option(name = "-input", metaVar = "[file]", usage = "topics path")
    public File input;

    @Option(name = "-path", metaVar = "[path]", required = true, usage = "index path")
    public Path path;

    @Option(name = "-output", metaVar = "[output]", required = true, usage = "output path")
    public Path output;

    @Option(name = "-encoding", metaVar = "[word]", required = true, usage = "encoding must be one of {fw, lexlsh}")
    public String encoding;

    @Option(name = "-depth", metaVar = "[int]", usage = "retrieval depth")
    public int depth = 10;

    @Option(name = "-lexlsh.n", metaVar = "[int]", usage = "n-grams")
    public int ngrams = 2;

    @Option(name = "-lexlsh.d", metaVar = "[int]", usage = "decimals")
    public int decimals = 1;

    @Option(name = "-lexlsh.hsize", metaVar = "[int]", usage = "hash set size")
    public int hashSetSize = 1;

    @Option(name = "-lexlsh.h", metaVar = "[int]", usage = "hash count")
    public int hashCount = 1;

    @Option(name = "-lexlsh.b", metaVar = "[int]", usage = "bucket count")
    public int bucketCount = 300;

    @Option(name = "-fw.q", metaVar = "[int]", usage = "quantization factor")
    public int q = 60;

    @Option(name = "-cutoff", metaVar = "[float]", usage = "tf cutoff factor")
    public float cutoff = 0.999f;

    @Option(name = "-msm", metaVar = "[float]", usage = "minimum should match")
    public float msm = 0f;
  }

  public static void main(String[] args) throws Exception {
    ApproximateNearestNeighborSearchCollection.Args indexArgs = new ApproximateNearestNeighborSearchCollection.Args();
    CmdLineParser parser = new CmdLineParser(indexArgs, ParserProperties.defaults().withUsageWidth(90));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println("Example: " + ApproximateNearestNeighborSearchCollection.class.getSimpleName() +
          parser.printExample(OptionHandlerFilter.REQUIRED));
      return;
    }
    Analyzer vectorAnalyzer;
    if (indexArgs.encoding.equalsIgnoreCase(FW)) {
      vectorAnalyzer = new FakeWordsEncoderAnalyzer(indexArgs.q);
    } else if (indexArgs.encoding.equalsIgnoreCase(LEXLSH)) {
      vectorAnalyzer = new LexicalLshAnalyzer(indexArgs.decimals, indexArgs.ngrams, indexArgs.hashCount,
          indexArgs.bucketCount, indexArgs.hashSetSize);
    } else {
      parser.printUsage(System.err);
      System.err.println("Example: " + ApproximateNearestNeighborSearchCollection.class.getSimpleName() +
          parser.printExample(OptionHandlerFilter.REQUIRED));
      return;
    }

    if (indexArgs.input == null) {
      System.err.println("-path must be set");
      return;
    }

    Path indexDir = indexArgs.path;
    if (!Files.exists(indexDir)) {
      Files.createDirectories(indexDir);
    }

    System.out.println(String.format("Reading index at %s", indexArgs.path));

    Directory d = FSDirectory.open(indexDir);
    DirectoryReader reader = DirectoryReader.open(d);
    IndexSearcher searcher = new IndexSearcher(reader);
    if (indexArgs.encoding.equalsIgnoreCase(FW)) {
      searcher.setSimilarity(new ClassicSimilarity());
    }

    Collection<String> vectorStrings = new LinkedList<>();
    System.out.println(String.format("Loading topics %s", indexArgs.input));

    LinkedHashMap<String, float[]> topicVectors = readTopics(indexArgs.input);

    PrintWriter out = new PrintWriter(Files.newBufferedWriter(indexArgs.output, StandardCharsets.US_ASCII));

    long start = System.currentTimeMillis();
    for (Map.Entry<String, float[]> entry : topicVectors.entrySet()) {
      float msm = indexArgs.msm;
      float cutoff = indexArgs.cutoff;
      CommonTermsQuery simQuery = new CommonTermsQuery(SHOULD, SHOULD, cutoff);
      
      StringBuilder sb = new StringBuilder();
      for (double fv : entry.getValue()) {
        if (sb.length() > 0) {
          sb.append(' ');
        }
        sb.append(fv);
      }
      String vectorString = sb.toString();
      
      for (String token : AnalyzerUtils.analyze(vectorAnalyzer, vectorString)) {
        simQuery.add(new Term(IndexVectors.FIELD_VECTOR, token));
      }
      if (msm > 0) {
        simQuery.setHighFreqMinimumNumberShouldMatch(msm);
        simQuery.setLowFreqMinimumNumberShouldMatch(msm);
      }

      TopScoreDocCollector results = TopScoreDocCollector.create(indexArgs.depth, Integer.MAX_VALUE);
      searcher.search(simQuery, results);

      //System.out.println(String.format("%d nearest neighbors of '%s':", indexArgs.depth, word));

      int rank = 1;
      for (ScoreDoc sd : results.topDocs().scoreDocs) {
        Document document = reader.document(sd.doc);
        String neighborWord = document.get(IndexVectors.FIELD_ID);
        //System.out.println(String.format("%d. %s (%.3f)", rank, neighborWord, sd.score));
        out.println(String.format(Locale.US, "%s Q0 %s %d %f %s",
            entry.getKey(), neighborWord, rank, sd.score, "Anserini-ANN"));
        rank++;
      }
    }
    long time = System.currentTimeMillis() - start;
    System.out.println(String.format("Search time: %dms", time));

    out.flush();
    out.close();
    reader.close();
    d.close();
  }

  static LinkedHashMap<String, float[]> readTopics(File input) throws IOException {
    LinkedHashMap<String, float[]> vectors = new LinkedHashMap<>();
    for (String line : IOUtils.readLines(new FileReader(input))) {
      String[] s = line.split("\\s+");
      if (s.length > 2) {
        String key = s[0];
        float[] vector = new float[s.length - 1];
        float norm = 0f;
        for (int i = 1; i < s.length; i++) {
          float f = Float.parseFloat(s[i]);
          vector[i - 1] = f;
          norm += Math.pow(f, 2);
        }
        norm = (float) Math.sqrt(norm);
        for (int i = 0; i < vector.length; i++) {
          vector[i] = vector[i] / norm;
        }
        vectors.put(key, vector);
      }
    }
    return vectors;
  }

}
