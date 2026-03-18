(ns hashcheck.core
  (:gen-class)
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.test :refer [deftest is]]
   [medley.core :as medley])
  (:import
   [java.io File FileInputStream]
   [java.security MessageDigest]
   [java.time LocalTime]
   [java.time.format DateTimeFormatter]))

(defn transduce-leaves
  ([root children reducer]
   (transduce-leaves root children reducer (reducer) identity))

  ([root children reducer initial-value]
   (transduce-leaves root children reducer initial-value identity))

  ([root children reducer initial-value transducer]
   (let [reducing-function (transducer reducer)]
     (loop [value initial-value
            inner-nodes [root]]
       (if-some [inner-node (first inner-nodes)]
         (let [[value inner-nodes] (loop [value value
                                          the-children (children inner-node)
                                          inner-nodes (rest inner-nodes)]
                                     (if-some [child (first the-children)]
                                       (if (children child)
                                         (recur value
                                                (rest the-children)
                                                (conj inner-nodes child))
                                         (let [result (reducing-function value
                                                                         child)]
                                           (if (reduced? result)
                                             (reducing-function @result)
                                             (recur result
                                                    (rest the-children)
                                                    inner-nodes))))
                                       [value inner-nodes]))]
           (recur value
                  inner-nodes))
         (reducing-function value))))))

(deftest test-transduce-leaves
  (is (= [:b :g :e]
         (transduce-leaves {:a :b
                            :c {:d :e}
                            :f :g}
                           (fn [value]
                             (if (map? value)
                               (vals value)
                               nil))
                           conj))))

(defn transduce-files! [directory-path transducer]
  (transduce-leaves (File. directory-path)
                    (fn [file]
                      (when (.isDirectory file)
                        (.listFiles file)))
                    (constantly nil)
                    nil
                    transducer))

(defn take-until-stopped [stopped?-atom]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (if @stopped?-atom
         (ensure-reduced result)
         (rf result input))))))

(defn md5 [stopped?-atom add-byte-count file-path]
  (let [digest (MessageDigest/getInstance "MD5" #_"SHA-256")]
    (with-open [fis (FileInputStream. (File. file-path))]
      (let [buffer (byte-array 8192)]
        (loop []
          (when @stopped?-atom
            (throw (ex-info "stopped" {:type :stopped})))
          (let [len (.read fis buffer)]
            (when (pos? len)
              (.update digest buffer 0 len)
              (add-byte-count len)
              (recur))))))
    (let [hash-bytes (.digest digest)]
      (apply str
             (map #(format "%02x" (bit-and % 255))
                  hash-bytes)))))

(defn read-hash-file [file-path]
  (with-open [reader (io/reader file-path)]
    (->> (line-seq reader)
         (map edn/read-string)
         (doall))))

(defn milliseconds-to-human-readable-units
  "Convert milliseconds to hours, minutes, and seconds."
  [milliseconds]
  (let [total-seconds (quot milliseconds 1000)
        remaining-seconds-after-hours (rem total-seconds 3600)]
    {:hours (quot total-seconds 3600)
     :minutes (quot remaining-seconds-after-hours 60)
     :seconds (rem remaining-seconds-after-hours 60)}))

(deftest test-milliseconds-to-human-readable-units
  (is (= {:hours 1, :minutes 2, :seconds 3}
         (milliseconds-to-human-readable-units (+ (* 60 60 1 1000)
                                                  (* 60 2 1000)
                                                  (* 3 1000))))))

(defn format-bytes [bytes]
  (let [units ["B" "KB" "MB" "GB" "TB" "PB" "EB" "ZB" "YB"]]
    (loop [size (double bytes)
           unit-index 0]
      (if (or (< size 1024) (>= unit-index (dec (count units))))
        (format (str "%." (min 2 unit-index) "f%s") size (units unit-index))
        (recur (/ size 1024) (inc unit-index))))))

(deftest test-format-bytes
  (is (= "130B"
         (format-bytes 130)))
  (is (= "1,3KB"
         (format-bytes 1300)))
  (is (= "1,24MB"
         (format-bytes 1300000)))
  (is (= "1,21GB"
         (format-bytes 1300000000))))

(defn current-time-stamp []
  (.format (LocalTime/now)
           (DateTimeFormatter/ofPattern "HH:mm:ss")))

(defn write-file-hashes
  "Write hashes to a file. The writing can be interrupted with ctrl-c
  and resumed later on.

  example:

  hashcheck write-file-hashes target target-hashes.edn

  To write hashes of only some part of the whole archive, add the
  subdirectory as the second argument:

  hashcheck write-file-hashes target target/classes target-hashes.edn"
  ([archive-directory-path output-file-path]
   (write-file-hashes archive-directory-path archive-directory-path output-file-path))
  ([archive-directory-path source-directory-path output-file-path]
   (let [stopped?-atom (atom false)
         error-file-path (str output-file-path
                              "-errors-"
                              (.format (java.time.LocalDateTime/now)
                                       (DateTimeFormatter/ofPattern "YYY-MM-dd-HH-mm-ss"))
                              ".edn")
         hash-file-rows (if (.exists (File. output-file-path))
                          (read-hash-file output-file-path)
                          [])
         previously-hashed-file-count (count hash-file-rows)
         previously-processed-byte-count (reduce + (map :byte-count hash-file-rows))
         already-hashed-files-set (->> hash-file-rows
                                       (map :path)
                                       (into #{}))
         archive-directory-path-length (count (.getAbsolutePath (File. archive-directory-path)))
         state-atom (atom {:written-hashes-count 0
                           :processed-files-count 0
                           :processed-byte-count 0
                           :currently-processed-path nil
                           :error-count 0})
         start-time (System/currentTimeMillis)
         reporting-interval-in-milliseconds 30000
         monitoring-thread (Thread. (fn []
                                      (loop [previously-reported-file-count 0
                                             previously-reported-byte-count 0]
                                        (let [state @state-atom]
                                          (try (Thread/sleep reporting-interval-in-milliseconds)
                                               (catch Throwable _throwable))
                                          (when (not @stopped?-atom)
                                            (println (current-time-stamp)
                                                     "processed"
                                                     (- (:processed-files-count state)
                                                        previously-reported-file-count)
                                                     "files"
                                                     (format-bytes (- (:processed-byte-count state)
                                                                      previously-reported-byte-count))
                                                     "bytes"
                                                     (format-bytes (:processed-byte-count state)) "in total"
                                                     (format-bytes (* (/ (- (:processed-byte-count state)
                                                                            previously-reported-byte-count)
                                                                         reporting-interval-in-milliseconds)
                                                                      1000 60 60))
                                                     "per hour"
                                                     (:processed-files-count state) "files in total"
                                                     previously-hashed-file-count
                                                     "files were already in the target file"
                                                     (:error-count state) "files have had errors.")

                                            (when (some? (:currently-processed-path state))
                                              (println (str "Currently processing: "
                                                            (:currently-processed-path state)
                                                            " ("
                                                            (format-bytes (.length (File. (str archive-directory-path
                                                                                               "/"
                                                                                               (:currently-processed-path state)))))
                                                            ")")))
                                            (recur (:processed-files-count state)
                                                   (:processed-byte-count state)))))))]

     (.start monitoring-thread)
     (with-open [writer (io/writer output-file-path :append true)]
       (.addShutdownHook (Runtime/getRuntime)
                         (Thread. (fn []
                                    (reset! stopped?-atom true)
                                    (.interrupt monitoring-thread)
                                    (Thread/sleep 200))))

       (transduce-files! source-directory-path
                         (comp (take-until-stopped stopped?-atom)
                               (map (fn [file]
                                      (swap! state-atom update :processed-files-count inc)
                                      (subs (.getAbsolutePath file)
                                            (inc archive-directory-path-length))))
                               (remove already-hashed-files-set)
                               (map (fn [path]
                                      (swap! state-atom assoc :currently-processed-path path)
                                      (try (.write writer
                                                   (let [processed-byte-count-before-hashing (:processed-byte-count @state-atom)
                                                         hash (md5 stopped?-atom
                                                                   (fn [processed-byte-count]
                                                                     (swap! state-atom update :processed-byte-count + processed-byte-count))
                                                                   (str archive-directory-path "/" path))]
                                                     (str (pr-str {:path path
                                                                   :md5 hash
                                                                   :byte-count (- (:processed-byte-count @state-atom)
                                                                                  processed-byte-count-before-hashing)})
                                                          "\n")))
                                           (.flush writer)
                                           (swap! state-atom update :written-hashes-count inc)
                                           (catch Throwable throwable
                                             (when (not (= :stopped (:type (ex-data throwable))))
                                               (swap! state-atom update :error-count inc)
                                               (println "there was an error when reading file" path ":" (.getMessage throwable))
                                               (spit error-file-path
                                                     (pr-str {:path path
                                                              :error-message (.getMessage throwable)})
                                                     :append true)))))))))

     (let [state @state-atom]
       (if @stopped?-atom
         (println "Got interrupted.")
         (println "All files were processed."))

       (reset! stopped?-atom true)
       (.interrupt monitoring-thread)
       (println "Wrote" (:written-hashes-count state) "hashes.")
       (println "Total runtime was" (milliseconds-to-human-readable-units (- (System/currentTimeMillis)

                                                                             start-time)))
       (let [processed-byte-count (:processed-byte-count state)]
         (println "Processed" (format-bytes processed-byte-count) ". Previously"

                  (format-bytes previously-processed-byte-count)
                  "were processed."

                  (format-bytes (* (/ processed-byte-count
                                      (- (System/currentTimeMillis)
                                         start-time))
                                   1000 60 60))
                  "per hour"

                  (format-bytes (+ previously-processed-byte-count
                                   processed-byte-count))
                  "in total."))

       (println (:error-count state) "files had errors.")
       (println previously-hashed-file-count "hashes were already in the target file.")))))

(defn differing-files [file-1-rows file-2-rows]
  (let [file-1-paths-to-hashes (->> file-1-rows
                                    (map (juxt :path :md5))
                                    (into {}))
        file-2-paths-to-hashes (->> file-2-rows
                                    (map (juxt :path :md5))
                                    (into {}))]
    (->> (set/intersection (set (map :path file-1-rows))
                           (set (map :path file-2-rows)))
         (remove (fn [path]
                   (= (get file-1-paths-to-hashes path)
                      (get file-2-paths-to-hashes path)))))))

(defn compare-hash-files
  "Sum up differences between hash files as numbers."
  [hash-file-1-path hash-file-2-path]
  (let [file-1-rows (read-hash-file hash-file-1-path)
        file-2-rows (read-hash-file hash-file-2-path)]
    (println "First file is missing" (count (set/difference (set (map :path file-2-rows))
                                                            (set (map :path file-1-rows))))
             "paths.")
    (println "Second file is missing" (count (set/difference (set (map :path file-1-rows))
                                                             (set (map :path file-2-rows))))
             "paths.")
    (println (->> (differing-files file-1-rows file-2-rows)
                  (count))
             "hashes are differing.")))

(defn list-differing-hashes
  "List files with differing hashes."
  [hash-file-1-path hash-file-2-path]
  (let [file-1-rows (read-hash-file hash-file-1-path)
        file-2-rows (read-hash-file hash-file-2-path)
        file-1-paths-to-hashes (->> file-1-rows
                                    (map (juxt :path :md5))
                                    (into {}))
        file-2-paths-to-hashes (->> file-2-rows
                                    (map (juxt :path :md5))
                                    (into {}))]

    (->> (differing-files file-1-rows
                          file-2-rows)
         (run! (fn [path]
                 (prn {:path path
                       :file-1-md5 (get file-1-paths-to-hashes path)
                       :file-2-md5 (get file-2-paths-to-hashes path)}))))))

(defn file-extension [file-name]
  (second (re-find #"\.([^\.]+)$"
                   file-name)))

(deftest test-file-extension
  (is (= "bar"
         (file-extension "foo.bar")))

  (is (= "bAr"
         (file-extension "foo.bAr")))

  (is (= "loo"
         (file-extension "foo/bar.baz.loo"))))


(defn statistics-for-rows [rows]
  {:count (count rows)
   :byte-count (reduce + (map :byte-count rows))})

(defn format-statistics [{:keys [count byte-count]}]
  (str count " files " (format-bytes byte-count) ))

(defn print-statistics-for-rows [rows]
  (println "total:" (format-statistics (statistics-for-rows rows)))
  (doseq [[file-extension statistics] (->> rows
                                           (group-by (comp file-extension string/lower-case :path))
                                           (medley/map-vals statistics-for-rows)
                                           (sort-by (comp :byte-count second))
                                           (reverse))]
    (println (str (or file-extension
                      "no file extension") ": " (format-statistics statistics)))))

(deftest test-print-statistics-for-rows
  (is (= "total: 3 files 14B
bar: 2 files 8B
no file extension: 1 files 6B
"
         (with-out-str (print-statistics-for-rows '({:path "sub1/hello",
                                                     :byte-count 6}
                                                    {:path "hello.bar",
                                                     :byte-count 3}
                                                    {:path "foo.bar",
                                                     :byte-count 5}))))))

(defn print-statistics
  "Print how many files and how many bytes there are by file extension."
  [hash-file-path]
  (print-statistics-for-rows (read-hash-file hash-file-path)))

(comment
  (read-hash-file "temp/file-hashes")
  (print-statistics "temp/file-hashes")
  (.delete (File. "temp/file-hashes"))
  (write-file-hashes "temp/hashtest" "temp/hashtest/sub1" "temp/file-hashes")
  (write-file-hashes "temp/hashtest" "temp/file-hashes")
  (write-file-hashes "temp/hashtest" "temp/file-hashes-2")
  (compare-hash-files "temp/file-hashes" "temp/file-hashes-2")
  (list-differing-hashes "temp/file-hashes" "temp/file-hashes-2")
  )

(def commands [#'write-file-hashes
               #'compare-hash-files
               #'list-differing-hashes
               #'print-statistics])

(defn -main [& command-line-arguments]
  (let [[command-name & arguments] command-line-arguments]
    (if-let [command (first (filter (fn [command]
                                      (= command-name
                                         (name (:name (meta command)))))
                                    commands))]
      (do (try (apply command arguments)
               (catch Exception exception
                 (println exception)))
          (.flush *out*)
          (shutdown-agents) ;; see https://clojureverse.org/t/why-doesnt-my-program-exit/3754/2
          )

      (do (println "Usage:\n")
          (println (->> commands
                        (map (fn [command-var]
                               (str (:name (meta command-var))
                                    ": "
                                    (:arglists (meta command-var))
                                    "\n\n  "

                                    (:doc (meta command-var)))))
                        (string/join "\n\n")))))))
