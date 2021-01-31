# pyspark_baitap2
# A. Spark RDD
## I. Khái niệm

RDD (Resilient Distributed Datasets) được định nghĩa trong Spark Core. Nó đại diện cho một collection các item đã được phân tán trên các cluster, và có thể xử lý phân tán. PySpark sử dụng PySpark RDDs và nó chỉ là 1 object của Python nên khi bạn viết code RDD transformations trên Java thực ra khi run, những transformations đó được ánh xạ lên object PythonRDD trên Java.
RDDs có thể chứa bất kỳ kiểu dữ liệu nào của Python, Java, hoặc đối tượng Scala, bao gồm các kiểu dữ liệu do người dùng định nghĩa. Thông thường, RDD chỉ cho phép đọc, phân mục tập hợp của các bản ghi. RDDs có thể được tạo ra qua điều khiển xác định trên dữ liệu trong bộ nhớ hoặc RDDs, RDD là một tập hợp có khả năng chịu lỗi mỗi thành phần có thể được tính toán song song.

Có hai cách để tạo RDDs:

  +	Tạo từ một tập hợp dữ liệu có sẵn trong ngôn ngữ sử dụng như Java, Python, Scala.
  +	Lấy từ dataset hệ thống lưu trữ bên ngoài như HDFS, Hbase hoặc các cơ sở dữ liệu quan hệ.
  
 ## II. Các đặc điểm của Spark RDD
 ### 1. Tính toán trong bộ nhớ
 
 Spark RDD cung cấp khả năng tính toán trong bộ nhớ. Nó lưu trữ các kết quả trung gian trong bộ nhớ phân tán (RAM) thay vì lưu trữ ổn định (đĩa).
 
 ### 2. Lazy Evaluations (đánh giá lười biếng)
 Tất cả các phép biến đổi trong Apache Spark đều được gọi là lười biếng (lazy), ở chỗ chúng không tính toán ngay kết quả của chúng. Thay vào đó, nó chỉ nhớ các phép biến đổi được áp dụng cho một số tập dữ liệu cơ sở.
 
Spark tính toán các phép biến đổi khi một hành động yêu cầu kết quả cho driver của chương trình.
### 3. Khả năng chịu lỗi

Spark RDD có khả năng chịu lỗi vì chúng theo dõi thông tin dòng dữ liệu để tự động xây dựng lại dữ liệu bị mất khi bị lỗi. Nó xây dựng lại dữ liệu bị mất khi lỗi bằng cách sử dụng dòng (lineage), mỗi RDD nhớ cách nó được tạo ra từ các tập dữ liệu khác (bằng các phép biến đổi như map, join hoặc GroupBy) để tạo lại chính nó.

### 4. Bất biến

Dữ liệu an toàn để chia sẻ trên các process. Ngoài ra, nó cũng có thể được tạo hoặc truy xuất bất cứ lúc nào giúp dễ dàng lưu vào bộ nhớ đệm, chia sẻ và nhân rộng. Vì vậy, chúng ta có thể sử dụng nó để đạt được sự thống nhất trong tính toán.

### 5. Phân vùng

Phân vùng là đơn vị cơ bản của tính song song trong Spark RDD. Mỗi phân vùng là một phân chia dữ liệu hợp lý mà có thể thay đổi được. Ta có thể tạo một phân vùng thông qua một số biến đổi trên các phân vùng hiện có.

### 6. Sự bền bỉ (persistence)

Người dùng có thể cho biết họ sẽ sử dụng lại những RDD nào và chọn hướng lưu trữ cho họ (ví dụ: lưu trữ trong bộ nhớ hoặc trên Đĩa).

### 7. Hoạt động chi tiết thô (Coarse-grained Operations)

Nó áp dụng cho tất cả các phần tử trong bộ dữ liệu thông qua map hoặc fiter hoặc group theo hoạt động.

### 8. Vị trí - Độ dính (Location – Stickiness)

RDD có khả năng xác định ưu tiên vị trí để tính toán các phân vùng. Tùy chọn vị trí đề cập đến thông tin về vị trí của RDD. DAGScheduler đặt các phân vùng theo cách sao cho tác vụ gần với dữ liệu nhất có thể. Do đó, tốc độ tính toán có thể tăng.

## III. Hoạt động trong Spark RDD (Spark RDD Operation)

### 1. Phân loại các loại hoạt động Spark RDD
RDD trong Apache Spark hỗ trợ hai loại hoạt động:
+ Transformation
+ Actions

#### Transformation

Spark RDD Transformations là các hàm sử dụng một RDD làm đầu vào và tạo ra một hoặc nhiều RDD làm đầu ra. Chúng ta không thay đổi RDD đầu vào (vì RDD là bất biến và do đó người ta không thể thay đổi nó), nhưng luôn tạo ra một hoặc nhiều RDD mới bằng cách áp dụng các tính toán mà nó đại diện.

Ví dụ: Map(), filter(), ReduceByKey(), ...

Các phép biến đổi là các hoạt động lười biếng trên RDD trong Apache Spark. Nó tạo ra một hoặc nhiều RDD mới, thực thi khi một Action xảy ra. Do đó, Transformation tạo ra một tập dữ liệu mới từ tập dữ liệu hiện có.

Một số phép biến đổi nhất định có thể được pipelined, đây là một phương pháp tối ưu hóa mà Spark sử dụng để cải thiện hiệu suất của các phép tính. Có hai loại phép biến hình: phép biến hình hẹp (narrow transformation), phép biến hình rộng(wide transformation).

##### Narrow Transformation

Đây là kết quả của ánh xạ, bộ lọc và sao cho dữ liệu chỉ từ một phân vùng duy nhất, tức là nó tự cung cấp. Một RDD đầu ra có các phân vùng với các bản ghi bắt nguồn từ một phân vùng duy nhất trong RDD mẹ. Chỉ một tập hợp con giới hạn của các phân vùng được sử dụng để tính toán kết quả.

Spark nhóm các phép biến hình thu hẹp dưới dạng một giai đoạn được gọi là pipelining.

<p align="center"> <img src ="https://d2h0cx97tjks2p.cloudfront.net/blogs/wp-content/uploads/sites/2/2017/08/spark-narrow-transformation-1.jpg" />
<p align="center"> Mô hình Narrow Transformation </p>

##### Wide Transformation

Đây là kết quả của các hàm như groupByKey() và ReduceByKey(). Dữ liệu cần thiết để tính các bản ghi trong một phân vùng có thể nằm trong nhiều phân vùng của RDD mẹ. Các phép biến đổi rộng còn được gọi là phép biến đổi trộn (shuffle transformations) vì chúng có thể có hoặc không phụ thuộc vào một lần trộn.

<p align="center"> <img src ="https://d2h0cx97tjks2p.cloudfront.net/blogs/wp-content/uploads/sites/2/2017/08/spark-wide-transformation.jpg" />
<p align="center"> Mô hình Wide Transformation </p>

#### Actions

Action trong Spark trả về kết quả cuối cùng của các tính toán RDD. Nó kích hoạt thực thi bằng cách sử dụng đồ thị dòng để tải dữ liệu vào RDD ban đầu, thực hiện tất cả các phép biến đổi trung gian và trả về kết quả cuối cùng cho chương trình Driver hoặc ghi nó ra hệ thống tệp. Đồ thị tuyến tính là đồ thị phụ thuộc của tất cả các RDD song song của RDD.

Các Actions là các hoạt động RDD tạo ra các giá trị không phải RDD. Chúng hiện thực hóa một giá trị trong chương trình Spark. Actions là một trong những cách để gửi kết quả từ người thực thi đến driver. First(), take(), Reduce(), collect(), count() là một số Action trong Spark.

Sử dụng các phép biến đổi (Transformations), người ta có thể tạo RDD từ biến hiện có. Nhưng khi chúng ta muốn làm việc với tập dữ liệu thực tế, tại thời điểm đó chúng ta sử dụng Action. Khi Hành động xảy ra, nó không tạo ra RDD mới, không giống như sự chuyển đổi. Do đó, Actions là các hoạt động RDD không cung cấp giá trị RDD. Actions lưu trữ giá trị của nó đối với driver hoặc hệ thống lưu trữ bên ngoài. Nó đưa sự lười biếng (lazy) của RDD vào chuyển động.

### 2. Code minh họa các hoạt động

Để áp dụng bất kỳ thao tác nào trong PySpark, trước tiên chúng ta cần tạo một PySpark RDD.

Khối mã sau có chi tiết về Lớp RDD của PySpark:

```python
      class pyspark.RDD (
         jrdd, 
         ctx, 
         jrdd_deserializer = AutoBatchedSerializer(PickleSerializer())
      )
```

Cách chạy một vài thao tác cơ bản bằng PySpark. Đoạn mã sau trong tệp Python tạo ra các từ RDD, lưu trữ một tập hợp các từ được đề cập

```python
      words = sc.parallelize (
         ["scala", 
         "java", 
         "hadoop", 
         "spark", 
         "akka",
         "spark vs hadoop", 
         "pyspark",
         "pyspark and spark"]
      )
```

+ Count(): Hàm count() cho biết số phần tử có trong RDD

```python
    from pyspark import SparkContext
    sc = SparkContext("local", "count app")
    words = sc.parallelize (
      ["scala", 
      "java", 
      "hadoop", 
      "spark", 
      "akka",
      "spark vs hadoop", 
      "pyspark",
      "pyspark and spark"]
)
    counts = words.count()
    print ("Number of elements in RDD -> %i" % (counts))
```

```note
    Number of elements in RDD → 8
```

+ collect(): trả về tất cả các phần tử ở trong RDD

```python
    from pyspark import SparkContext
    sc = SparkContext("local", "Collect app")
    words = sc.parallelize (
       ["scala", 
       "java", 
       "hadoop", 
       "spark", 
       "akka",
       "spark vs hadoop", 
       "pyspark",
       "pyspark and spark"]
      )
    coll = words.collect()
    print ("Elements in RDD -> %s" % (coll))
 ```
```note
    Elements in RDD -> [
       'scala', 
       'java', 
       'hadoop', 
       'spark', 
       'akka', 
       'spark vs hadoop', 
       'pyspark', 
       'pyspark and spark'
    ]
 ```
+ map(f, securePartitioning = False): Một RDD mới được trả về bằng cách áp dụng một hàm cho mỗi phần tử trong RDD. Trong ví dụ sau, chúng ta sẽ tạo một cặp giá trị khóa và ánh xạ mọi chuỗi với giá trị 1.

```python
    from pyspark import SparkContext
    sc = SparkContext("local", "Map app")
    words = sc.parallelize (
       ["scala", 
       "java", 
       "hadoop", 
       "spark", 
       "akka",
       "spark vs hadoop", 
       "pyspark",
       "pyspark and spark"]
    )
    words_map = words.map(lambda x: (x, 1))
    mapping = words_map.collect()
    print("Key value pair -> %s" % (mapping))
```

```note
    Key value pair -> [
       ('scala', 1), 
       ('java', 1), 
       ('hadoop', 1), 
       ('spark', 1), 
       ('akka', 1), 
       ('spark vs hadoop', 1), 
       ('pyspark', 1), 
       ('pyspark and spark', 1)
    ]
 ```
 
+ reduce(f): Sau khi thực hiện thao tác nhị phân giao hoán và kết hợp được chỉ định, phần tử trong RDD được trả về. Trong ví dụ sau, chúng tôi đang nhập gói thêm từ toán tử và áp dụng nó trên 'num' để thực hiện một thao tác thêm đơn giản.
 
 ```python
    from pyspark import SparkContext
    from operator import add
    sc = SparkContext("local", "Reduce app")
    nums = sc.parallelize([1, 2, 3, 4, 5])
    adding = nums.reduce(add)
    print ("Adding all the elements -> %i" % (adding))
 ```
 
 ```note
 Adding all the elements -> 15
 ```
 
+ join(other, numPartitions = none): Trả về RDD với một cặp phần tử với các khóa phù hợp và tất cả các giá trị cho khóa cụ thể đó. Trong ví dụ sau, có hai cặp phần tử trong hai RDD khác nhau. Sau khi kết hợp hai RDD này, chúng ta nhận được một RDD với các phần tử có khóa phù hợp và giá trị của chúng.

```python
from pyspark import SparkContext
sc = SparkContext("local", "Join app")
x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print ("Join RDD -> %s" % (final))
```

```note
Join RDD -> [
   ('spark', (1, 2)),  
   ('hadoop', (4, 5))
]
```

## IV. Thực thi trên MapReduce

MapReduce được áp dụng rộng rãi để xử lý và tạo các bộ dữ liệu lớn với thuật toán xử lý phân tán song song trên một cụm. Nó cho phép người dùng viết các tính toán song song, sử dụng một tập hợp các toán tử cấp cao, mà không phải lo lắng về xử lý/phân phối công việc và khả năng chịu lỗi.

Tuy nhiên, trong hầu hết các framework hiện tại, cách duy nhất để sử dụng lại dữ liệu giữa các tính toán (Ví dụ: giữa hai công việc MapReduce) là ghi nó vào storage (Ví dụ: HDFS). Mặc dù framework này cung cấp nhiều hàm thư viện để truy cập vào tài nguyên tính toán của cụm Cluster, điều đó vẫn là chưa đủ.

Cả hai ứng dụng Lặp (Iterative) và Tương tác (Interactive) đều yêu cầu chia sẻ truy cập và xử lý dữ liệu nhanh hơn trên các công việc song song. Chia sẻ dữ liệu chậm trong MapReduce do sao chép tuần tự và tốc độ I/O của ổ đĩa. Về hệ thống lưu trữ, hầu hết các ứng dụng Hadoop, cần dành hơn 90% thời gian để thực hiện các thao tác đọc-ghi HDFS.

<p align="center"> <img src ="https://www.wailian.work/images/2019/05/30/iterative_operations_on_mapreduce-min.jpg" />
<p align="center"> Iterative Operation trên MapReduce </p>

<p align="center"> <img src ="https://www.researchgate.net/profile/Mehdi_Ben_Hamida/publication/326572328/figure/fig10/AS:651828509282310@1532419435733/Interactive-operations-on-MapReduce.png" />
<p align="center"> Interactive Operations trên MapReduce </p>

## V. Thực thi trên Spark RDD

Để khắc phục được vấn đề về MapRedure, các nhà nghiên cứu đã phát triển một framework chuyên biệt gọi là Apache Spark. Ý tưởng chính của Spark là Resilient Distributed Datasets (RDD); nó hỗ trợ tính toán xử lý trong bộ nhớ. Điều này có nghĩa, nó lưu trữ trạng thái của bộ nhớ dưới dạng một đối tượng trên các công việc và đối tượng có thể chia sẻ giữa các công việc đó. Việc xử lý dữ liệu trong bộ nhớ nhanh hơn 10 đến 100 lần so với network và disk.

<p align="center"> <img src ="https://www.researchgate.net/profile/Mehdi_Ben_Hamida/publication/326572328/figure/fig11/AS:651828509282311@1532419435803/Iterative-operations-on-Spark-RDD.png" />
<p align="center"> Iterative Operation trên Spark RDD </p>

<p align="center"> <img src ="https://www.researchgate.net/profile/Mehdi_Ben_Hamida/publication/326572328/figure/fig12/AS:651828509282313@1532419435877/Interactive-operations-on-Spark-RDD.png" />
<p align="center"> Interactive Operation trên Spark RDD </p>




 
 # B. Spark Dataframe
 ## I. Khái niệm
 
Khung dữ liệu (dataframe) là một bảng hoặc cấu trúc giống như mảng hai chiều, trong mà mỗi cột chứa các phép đo trên một biến và mỗi hàng chứa một trường hợp.

Vì vậy, một DataFrame có siêu dữ liệu bổ sung do định dạng bảng của nó, cho phép Spark chạy một số tối ưu hóa nhất định trên truy vấn đã hoàn thành. 

Mặt khác, RDD theo như chúng ta biết chỉ là một Resilient Distribution Dataset có nhiều hộp đen dữ liệu không thể được tối ưu hóa như các hoạt động có thể được thực hiện chống lại nó, không bị ràng buộc.

Tuy nhiên, chúng ta có thể chuyển từ DataFrame sang RDD thông qua phương thức rdd của nó và ngược lại có thể chuyển từ RDD sang DataFrame (nếu RDD ở định dạng bảng) thông qua phương thức toDF.

Nhìn chung, chúng ta nên sử dụng DataFrame trong trường hợp có thể do tối ưu hóa truy vấn tích hợp.

## II. Một số tính năng của Dataframe và nguồn dữ liệu PySpark
### 1. Tính năng

DataFrame được phân phối trong tự nhiên, làm cho nó trở thành một cấu trúc dữ liệu có khả năng chịu lỗi và có tính khả dụng cao.

Đánh giá lười biếng là một chiến lược đánh giá giữ việc đánh giá một biểu thức cho đến khi giá trị của nó là cần thiết. Nó tránh đánh giá lặp lại. Đánh giá lười biếng trong Spark có nghĩa là quá trình thực thi sẽ không bắt đầu cho đến khi một hành động được kích hoạt. Trong Spark, bức tranh về sự lười biếng xuất hiện khi các phép biến đổi Spark xảy ra.

### 2. Nguồn dữ liệu PySpark

Dữ liệu có thể được tải vào thông qua tệp CSV, JSON, XML hoặc tệp Parquet. Nó cũng có thể được tạo bằng cách sử dụng RDD hiện có và thông qua bất kỳ cơ sở dữ liệu nào khác, như Hive hoặc Cassandra . Nó cũng có thể lấy dữ liệu từ HDFS hoặc hệ thống tệp cục bộ.

### 3. Một số lợi ích khi sử dụng Spark Dataframe

+ Xử lý dữ liệu có cấu trúc và bán cấu trúc: DataFrames được thiết kế để xử lý một tập hợp lớn dữ liệu có cấu trúc cũng như bán cấu trúc . Các quan sát trong Spark DataFrame được tổ chức dưới các cột được đặt tên, giúp Apache Spark hiểu được lược đồ của Dataframe. Điều này giúp Spark tối ưu hóa kế hoạch thực thi trên các truy vấn này. Nó cũng có thể xử lý hàng petabyte dữ liệu.

+ Slicing và Dicing: API DataFrames thường hỗ trợ các phương pháp phức tạp để cắt và phân loại dữ liệu. Nó bao gồm các hoạt động như "selecting" hàng, cột và ô theo tên hoặc theo số, lọc ra các hàng, v.v. Dữ liệu thống kê thường rất lộn xộn và chứa nhiều giá trị bị thiếu và không chính xác cũng như vi phạm phạm vi. Vì vậy, một tính năng cực kỳ quan trọng của DataFrames là quản lý rõ ràng dữ liệu bị thiếu.

+ Hỗ trợ nhiều ngôn ngữ: Hỗ trợ API cho các ngôn ngữ khác nhau như Python, R, Scala, Java, giúp những người có nền tảng lập trình khác nhau sử dụng dễ dàng hơn. 

+	Nguồn dữ liệu: DataFrames có hỗ trợ cho nhiều định dạng và nguồn dữ liệu, chúng ta sẽ xem xét vấn đề này sau trong hướng dẫn Pyspark DataFrames này. Họ có thể lấy dữ liệu từ nhiều nguồn khác nhau.

# C. Spark Properties
## I. Giới thiệu về Spark Properties

Spark Properties giúp chúng ta kiểm soát hầu hết các cài đặt ứng dụng và được cấu hình riêng cho từng ứng dụng. Các thuộc tính này có thể được đặt trực tiếp trên SparkConf và được chuyển tới SparkContext. SparkConf cho phép ta cấu hình một số thuộc tính phổ biến (ví dụ: URL chính và tên ứng dụng), cũng như các cặp khóa - giá trị tùy ý thông qua phương thức set ().

Ví dụ: Tạo một ứng dụng với 2 luồng

```python
    val conf = new SparkConf()
                 .setMaster("local[2]")
                 .setAppName("CountingSheep")
    val sc = new SparkContext(conf)
```

Trong đó, local[2] cho biết tối thiểu có 2 luồng đang chạy song song, giúp phát hiện lỗi chỉ tồn tại khi chạy trong bối cảnh phân tán.

Các định dạng thuộc tính kích thước byte có trong Spark:
1b (bytes)

1k or 1kb (kibibytes = 1024 bytes)

1m or 1mb (mebibytes = 1024 kibibytes)

1g or 1gb (gibibytes = 1024 mebibytes)

1t or 1tb (tebibytes = 1024 gibibytes)

1p or 1pb (pebibytes = 1024 tebibytes)

## II. Tải động các thuộc tính Spark (Dynamically Loading Spark Properties)

Trong một số trường hợp, ta có thể muốn tránh mã hóa cứng các cấu hình nhất định trong SparkConf.

Ví dụ: nếu bạn muốn chạy cùng một ứng dụng với các bản chính khác nhau hoặc số lượng bộ nhớ khác nhau. Spark cho phép chúng ta chỉ cần tạo một conf trống:

```python
    val sc = new SparkContext(new SparkConf())
```

Sau đó, cung cấp các giá trị cấu hình trong lúc chạy Spark:

```python
    ./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false
      --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" myApp.jar
```

Trong đó, công cụ spark-submit và The Spark shell hỗ trợ hai cách để tải cấu hình động.

Đầu tiên là các tùy chọn dòng lệnh, chẳng hạn như --master, như được hiển thị ở trên. Spark-submit có thể chấp nhận bất kỳ thuộc tính Spark nào bằng cách sử dụng --conf/-c flag, nhưng sử dụng flag đặc biệt cho các thuộc tính đóng một vai trò trong việc khởi chạy ứng dụng Spark. Chạy ./bin/spark-submit –help sẽ hiển thị toàn bộ danh sách các tùy chọn này.

## III. View của Spark Properties

Giao diện người dùng web ứng dụng tại http: // <driver>: 4040 liệt kê các thuộc tính Spark trong tab "Environment". Đây là một nơi hữu ích để kiểm tra, đảm bảo rằng các thuộc tính đã được đặt chính xác.

Lưu ý: chỉ các giá trị được chỉ định rõ ràng thông qua spark-defaults.conf, SparkConf hoặc dòng lệnh mới xuất hiện. Đối với tất cả các thuộc tính cấu hình khác, ta có thể giả sử giá trị mặc định được sử dụng.

## IV. Spark Properties

Properties của Spark chủ yếu được chia thành hai loại:

+ Liên quan đến triển khai: như spark.driver.memory, spark.executor.instances. Loại thuộc tính này có thể không bị ảnh hưởng khi thiết lập theo chương trình SparkConf trong thời gian chạy hoặc hành vi tùy thuộc vào trình quản lý cụm và chế độ triển khai đã chọn trước. Do đó nên đặt thông qua file cấu trúc hoặc tùy chọn dòng lệnh spark-submit.

+ Liên quan đến kiểm soát thời gian chạy Spark: như spark.task.maxFailures.

### 1. Một số properties ứng dụng - Application Properties

<p align="center"> <img src ="https://user-images.githubusercontent.com/77916314/106388337-99f29b80-6410-11eb-8678-6249042bde2e.png" />
  
### 2. Một số properties xáo trộn - Shuffle Properties

<p align="center"> <img src ="https://user-images.githubusercontent.com/77916314/106388478-43399180-6411-11eb-9480-78a2afd844ac.png" />

### 3. Giao diện người dùng Spark UI

<p align="center"> <img src ="https://user-images.githubusercontent.com/77916314/106388534-8267e280-6411-11eb-8716-81b25446256c.png" />

### 4. Các properties khác

Ngoài các loại thuộc tính trên Spark còn hỗ trợ nhiều loại thuộc tính khác nhau:

+ Môi trường thực thi (Runtime Environment)
+ Quản lý bộ nhớ (Memory Management)
+ Hành vi thực thi (Execution Behavior)
+ Chỉ số thực thi (Executor Metrics)
+ Kết nối mạng (Networking)
+ Lập lịch (Scheduling)
+ Chế độ thực thi rào cản (Barrier Execution Mode)
+ Phân bố động (Dynamic Allocation)
+ Cấu hình Thread (Thread Configurations)
+ Bảo mật (Security)

# D. Tài liệu tham khảo

1.	https://spark.apache.org/docs/latest/configuration.html#spark-properties
2.	https://data-flair.training/blogs/spark-rdd-tutorial/
3.	https://codetudau.com/xu-ly-du-lieu-voi-spark-dataframe/index.html
4.	https://laptrinhx.com/huong-dan-pyspark-dataframe-gioi-thieu-ve-dataframes-367277857/
5.	https://www.tutorialspoint.com/pyspark/pyspark_rdd.htm

