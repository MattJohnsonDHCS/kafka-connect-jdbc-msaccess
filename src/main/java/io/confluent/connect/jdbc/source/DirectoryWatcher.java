/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * DirectoryWatcher is a TimerTask class that periodically watches for changes on a
 * defined directory. Every detected file is put into a queue waiting
 * to be sent to Kafka.
 */
public class DirectoryWatcher implements Runnable {
  private String path;
  private File[] filesArray;
  private HashMap dir = new HashMap();
  private AccessFileFilter fileFilter;
  private ConcurrentLinkedQueue<File> queuedFiles;



  /**
   * Constructor of the class.
   *
   * @para path directory path to watch
   **/
  @SuppressWarnings("unchecked")
  public DirectoryWatcher(String path) {
    this.path = path;
    fileFilter = new AccessFileFilter();
    filesArray = new File(path).listFiles(fileFilter);

    queuedFiles = new ConcurrentLinkedQueue<>();
    // transfer to the hashmap be used a reference and keep the
    // lastModified value
    for (File file : filesArray) {
      dir.put(file, file.lastModified());
    }
  }


  /**
   * Run the thread.
   */
  @SuppressWarnings("unchecked")
  @Override
  public final void run() {
    HashSet checkedFiles = new HashSet();
    filesArray = new File(path).listFiles(fileFilter);

    // scan the files and check for modification/addition
    for (File file : filesArray) {
      Long current = (Long) dir.get(file);
      checkedFiles.add(file);
      if (current == null) {
        // new file
        dir.put(file, new Long(file.lastModified()));
        queuedFiles.add(file);
      }
    }

    // now check for deleted files
    Set ref = ((HashMap)dir.clone()).keySet();
    ref.removeAll((Set)checkedFiles);
    for (Object o : ref) {
      File deletedFile = (File) o;
      dir.remove(deletedFile);
    }
  }


  /**
   * Expose the files queue
   */
  public ConcurrentLinkedQueue<File> getQueuedFiles() {
    return queuedFiles;
  }
}