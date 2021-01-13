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

package io.anserini.search.topicreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Topic reader for CLEF SGML topics with title, description, and narrative fields.
 */
public class ClefTopicReader extends TopicReader<Integer> {

  private final String newline = System.getProperty("line.separator");

  public ClefTopicReader(Path topicFile) {
    super(topicFile);
  }

  // read until finding a line that starts with the specified prefix
  protected StringBuilder read(BufferedReader reader, String prefix, StringBuilder sb,
                               boolean collectMatchLine, boolean collectAll) throws IOException {
    sb = (sb == null ? new StringBuilder() : sb);
    String sep = "";
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        return null;
      }
      if (line.startsWith(prefix)) {
        if (collectMatchLine) {
          sb.append(sep + line);
          sep = newline;
        }
        break;
      }
      if (collectAll) {
        sb.append(sep + line);
        sep = newline;
      }
    }
    return sb;
  }

  protected String extract(BufferedReader reader) throws IOException {
    String lang;

    int length = 1000;
    char buffer[] = new char[length];
    reader.mark(length);
    reader.read(buffer, 0, length);
    reader.reset();

    Pattern pattern = Pattern.compile("<([A-Z]{2})-title>");
    Matcher match = pattern.matcher(new String(buffer));
    if (match.find()) {
      lang = match.group(1);
    } else {
      throw new IOException("Cannot find field like <EN-title>");
    }

    return lang;
  }

  @Override
  public SortedMap<Integer, Map<String, String>> read(BufferedReader bRdr) throws IOException {
    SortedMap<Integer, Map<String, String>> map = new TreeMap<>();
    StringBuilder sb;
    try {
      // CLEF SGML uses tags like EN-title or RU-title and we need the lang code
      String lang = extract(bRdr);

      // CLEF SGML topics begin with <top>
      while (null != (sb = read(bRdr, "<top", null, false, false))) {
        Map<String, String> fields = new HashMap<>();
        // Read the topic id
        sb = read(bRdr, "<num>", null, true, false);
        int k = sb.indexOf(">");
        String id = sb.substring(k + 1);
        id = id.replaceAll("</num>", "").trim();

        // title
        sb = read(bRdr, "<" + lang + "-title>", null, true, false);
        k = sb.indexOf(">");
        String title = sb.substring(k + 1).trim();

        // Read the description...
        read(bRdr, "<" + lang + "-desc>", null, false, false);
        sb.setLength(0);
        String line = null;
        while ((line = bRdr.readLine()) != null) {
          if (line.startsWith("<" + lang + "-narr>"))
            break;
          if (sb.length() > 0) sb.append(' ');
          sb.append(line);
        }
        String description = sb.toString().trim();

        // Read the narrative...
        sb.setLength(0);
        if (line.endsWith("</" + lang + "-narr>")) {
          // This means that the narrative is on a single line, like '<EN-narr>....</EN-narr>'
          sb.append(line);
        } else {
          // Otherwise, read until closing '</top>' tag.
          while ((line = bRdr.readLine()) != null) {
            if (line.startsWith("</top>"))
              break;
            if (sb.length() > 0) sb.append(' ');
            sb.append(line);
          }
        }
        String narrative = sb.toString().trim();

        // clean up and save topic
        id = id.replaceAll("[^0-9]", "");
        title = title.replaceAll("</" + lang + "-title>", "").trim();
        description = description.replaceAll("</" + lang + "-desc>", "").trim();
        narrative = narrative.replaceAll("</" + lang + "-narr>", "").trim();
        fields.put("title", title);
        fields.put("description", description);
        fields.put("narrative", narrative);

        map.put(Integer.valueOf(id), fields);
      }
    } finally {
      bRdr.close();
    }

    return map;
  }
}