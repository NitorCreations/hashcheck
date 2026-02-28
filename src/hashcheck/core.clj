(ns hashcheck.core
  (:gen-class)
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.test :refer [deftest is]])
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

(defn md5 [stopped?-atom processed-byte-count-atom file-path]
  (let [digest (MessageDigest/getInstance "MD5" #_"SHA-256")]
    (with-open [fis (FileInputStream. (File. file-path))]
      (let [buffer (byte-array 8192)]
        (loop []
          (when @stopped?-atom
            (throw (ex-info "stopped" {:type :stopped})))
          (let [len (.read fis buffer)]
            (when (pos? len)
              (.update digest buffer 0 len)
              (swap! processed-byte-count-atom + len)
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

(defn write-file-hashes [directory-path output-file-path]
  (let [stopped?-atom (atom false)
        hash-file-rows (if (.exists (File. output-file-path))
                         (read-hash-file output-file-path)
                         [])
        previously-processed-byte-count (reduce + (map :byte-count hash-file-rows))
        already-hashed-files-set (->> hash-file-rows
                                      (map :path)
                                      (into #{}))
        directory-path-length (count (.getAbsolutePath (File. directory-path)))
        written-hashes-count-atom (atom 0)
        processed-files-count-atom (atom 0)
        error-count-atom (atom 0)
        start-time (System/currentTimeMillis)
        currently-processed-path-path (atom nil)
        processed-byte-count-atom (atom 0)
        reporting-interval-in-milliseconds 6000]
    (with-open [writer (io/writer output-file-path :append true)]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. (fn []
                                   (reset! stopped?-atom true)
                                   (Thread/sleep 1000))))
      (.start (Thread. (fn []
                         (loop [previously-reported-file-count 0
                                previously-reported-byte-count 0]
                           (let [processed-files-count @processed-files-count-atom
                                 processed-byte-count @processed-byte-count-atom]
                             (Thread/sleep reporting-interval-in-milliseconds)
                             (when (not @stopped?-atom)
                               (println (current-time-stamp)
                                        "processed"
                                        (- processed-files-count
                                           previously-reported-file-count)
                                        "files"
                                        (format-bytes (- processed-byte-count
                                                         previously-reported-byte-count))
                                        "bytes"
                                        (format-bytes processed-byte-count) " in total"
                                        (format-bytes (* (/ (- processed-byte-count
                                                               previously-reported-byte-count)
                                                            reporting-interval-in-milliseconds)
                                                         1000 60 60))
                                        "per hour"
                                        processed-files-count "files in total"
                                        (- processed-files-count
                                           @written-hashes-count-atom)
                                        "files were already in the target file")

                               #_(reset! processed-byte-count-atom 0)

                               (when (some? @currently-processed-path-path)
                                 (println (str "Currently processing: "
                                               @currently-processed-path-path
                                               " ("
                                               (format-bytes (.length (File. (str directory-path
                                                                                  "/"
                                                                                  @currently-processed-path-path))))
                                               ")")))
                               (recur processed-files-count
                                      processed-byte-count)))))))
      (transduce-files! directory-path
                        (comp (take-until-stopped stopped?-atom)
                              (map (fn [file]
                                     (swap! processed-files-count-atom inc)
                                     (subs (.getAbsolutePath file)
                                           (inc directory-path-length))))
                              (remove already-hashed-files-set)
                              (map (fn [path]
                                     (reset! currently-processed-path-path path)
                                     (try (.write writer
                                                  (let [processed-byte-count-before-hashing @processed-byte-count-atom
                                                        hash (md5 stopped?-atom
                                                                  processed-byte-count-atom
                                                                  (str directory-path "/" path))]
                                                    (str (pr-str {:path path
                                                                  :md5 hash
                                                                  :byte-count (- @processed-byte-count-atom
                                                                                 processed-byte-count-before-hashing)})
                                                         "\n")))
                                          (.flush writer)
                                          (swap! written-hashes-count-atom inc)
                                          (catch Throwable throwable
                                            (when (not (= :stopped (:type (ex-data throwable))))
                                              (swap! error-count-atom inc)
                                              (println "there was an error when reading file" path ":" (.getMessage throwable))))))))))
    (if @stopped?-atom
      (println "Got interrupted.")
      (println "All files were processed."))
    (println "Wrote" @written-hashes-count-atom "hashes.")
    (println "Processed" (format-bytes @processed-byte-count-atom) ". Previously"
             (format-bytes previously-processed-byte-count) "were processed."
             (format-bytes (+ previously-processed-byte-count @processed-byte-count-atom)) "in total.")
    (println "Total runtime was" (milliseconds-to-human-readable-units (- (System/currentTimeMillis)
                                                                          start-time)))
    (println @error-count-atom "files had errors.")
    (when (not (= @written-hashes-count-atom
                  @processed-files-count-atom))
      (println (- @processed-files-count-atom
                  @written-hashes-count-atom) "hashes were already in the target file."))))

(defn compare-hash-files [file-1-path file-2-path]
  (let [file-1-rows (read-hash-file file-1-path)
        file-2-rows (read-hash-file file-2-path)]
    (println "First file is missing" (count (set/difference (set (map :path file-2-rows))
                                                            (set (map :path file-1-rows))))
             "paths.")
    (println "Second file is missing" (count (set/difference (set (map :path file-1-rows))
                                                             (set (map :path file-2-rows))))
             "paths.")
    (println (let [file-1-paths-to-hashes (->> file-1-rows
                                               (map (juxt :path :md5))
                                               (into {}))
                   file-2-paths-to-hashes (->> file-2-rows
                                               (map (juxt :path :md5))
                                               (into {}))]
               (->> (set/intersection (set (map :path file-1-rows))
                                      (set (map :path file-2-rows)))
                    (remove (fn [path]
                              (= (get file-1-paths-to-hashes path)
                                 (get file-2-paths-to-hashes path))))
                    (count)))
             "hashes are differing.")))

(defn list-differing-hashes [file-1-path file-2-path]
  (let [file-1-rows (read-hash-file file-1-path)
        file-2-rows (read-hash-file file-2-path)
        file-1-paths-to-hashes (->> file-1-rows
                                    (map (juxt :path :md5))
                                    (into {}))
        file-2-paths-to-hashes (->> file-2-rows
                                    (map (juxt :path :md5))
                                    (into {}))]
    (->> (set/intersection (set (map :path file-1-rows))
                           (set (map :path file-2-rows)))
         (remove (fn [path]
                   (= (get file-1-paths-to-hashes path)
                      (get file-2-paths-to-hashes path))))
         (map (fn [path]
                {:path path
                 :file-1-md5 (get file-1-paths-to-hashes path)
                 :file-2-md5 (get file-2-paths-to-hashes path)})))))



(comment
  (.delete (File. "temp/file-hashes"))
  (write-file-hashes "temp/hashtest" "temp/file-hashes")
  (write-file-hashes "temp/hashtest" "temp/file-hashes-2")
  (compare-hash-files "temp/file-hashes" "temp/file-hashes-2")
  (list-differing-hashes "temp/file-hashes" "temp/file-hashes-2")
  )

(def commands [#'write-file-hashes
               #'compare-hash-files
               #'list-differing-hashes])

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

      (do (println "Usage:")
          (println "------------------------")
          (println (->> commands
                        (map (fn [command-var]
                               (str (:name (meta command-var))
                                    ": "
                                    (:arglists (meta command-var))
                                    "\n"
                                    (:doc (meta command-var)))))
                        (string/join "------------------------\n")))))))
