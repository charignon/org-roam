(require '[clojure.java.shell :refer [sh]])
(require '[clojure.java.io :as io])
(require '[cheshire.core :as json])

(defn title-raw [k]
  (second (first (re-seq #"(?i)#\+TITLE: (.*)" k))))

(defn alias-raw [k]
  (let [m (re-seq #"(?i)#\+roam_alias: (.*)" k)]
    (if m
      (json/parse-string (str "[" (str/replace (second (first m)) #"\"\s+\"" "\",\"") "]"))
    [])))

(defn links-raw [k]
  (apply hash-map (flatten (map next (re-seq #"\[\[file:([^\]]+)\]\[([^\]]+)\]\]" k)))))

(defn sha1
  [s]
  (let [hashed (.digest (java.security.MessageDigest/getInstance "SHA-1")
                        (.getBytes s))
        sw (java.io.StringWriter.)]
    (binding [*out* sw]
      (doseq [byte hashed]
        (print (format "%02x" byte))))
    (str sw)))

(defn to-link-storage [l]
  (pr-str (list :outline nil :content (second l) :point 1)))

(defn to-storage-time [t]
  (let [t2 (list (bit-shift-right t 16) (bit-and t 0xFFFF) 0 0)]
    (pr-str (list :atime t2 :mtime t2))))

(defn in-mem-db [roam-dir] (->> (file-seq  (io/file roam-dir))
              (pmap #(let [fname (.getAbsolutePath %1)]
                       (when (str/ends-with? fname ".org")
                         (let [c (slurp fname)
                               lm (int (/ (.lastModified %1) 1000))]
                           (-> {:fname (pr-str fname)
                                :alias (map pr-str (alias-raw c))
                                :hash (pr-str (sha1 c))
                                :meta (to-storage-time lm)
                                :title (pr-str (title-raw c))
                                :links (links-raw c)})))))
              (keep identity)))

(defn write-csv [title f]
  (with-open [writer (io/writer title)]
          (csv/write-csv writer (doall (f)))))

(defn titles-db [db]
  (for [e db
        t (cons (:title e) (:alias e))]
        [(:fname e) t]))

(defn files-db [db]
  (map (juxt :fname :hash :meta) db))


(defn links-db [roam-dir db]
  (for [e db
        l (:links e)]
    [(:fname e) (pr-str (format "%s%s" roam-dir (first l))) (pr-str "file") (to-link-storage l)]))

(def db-init-str "
CREATE TABLE IF NOT EXISTS files (file UNIQUE PRIMARY KEY, hash NOT NULL, meta NOT NULL);
CREATE TABLE IF NOT EXISTS headlines (id UNIQUE PRIMARY KEY, file NOT NULL);
  CREATE TABLE IF NOT EXISTS links (\"from\" NOT NULL, \"to\" NOT NULL, type NOT NULL, properties NOT NULL);
CREATE TABLE IF NOT EXISTS refs (ref UNIQUE NOT NULL, file NOT NULL, type NOT NULL);
CREATE TABLE IF NOT EXISTS tags (file UNIQUE PRIMARY KEY, tags);
CREATE TABLE IF NOT EXISTS titles (file NOT NULL, title );
PRAGMA user_version = 7;
.mode csv
.import /tmp/files.csv files
.import /tmp/links.csv links
.import /tmp/titles.csv titles")

(time
 (let [roam-dir (first *command-line-args*)
       roam-db-path (format "%sorg-roam.db" roam-dir)
       db (in-mem-db roam-dir)]
   (doall (pmap
           #(apply write-csv %1)
           [["/tmp/titles.csv" #(titles-db db)]
            ["/tmp/links.csv" #(links-db roam-dir db)]
            ["/tmp/files.csv" #(files-db db)]]))
   (sh "rm" roam-db-path)
   (sh "sqlite3" roam-db-path :in db-init-str)
   (println (format "DONE importing %d files" (count db)))))
