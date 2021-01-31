# pyspark_baitap2
# Spark RDD
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

RDD trong Apache Spark hỗ trợ hai loại hoạt động:
+ Transformation
+ Actions

### 1. Transformation

Spark RDD Transformations là các hàm sử dụng một RDD làm đầu vào và tạo ra một hoặc nhiều RDD làm đầu ra. Chúng ta không thay đổi RDD đầu vào (vì RDD là bất biến và do đó người ta không thể thay đổi nó), nhưng luôn tạo ra một hoặc nhiều RDD mới bằng cách áp dụng các tính toán mà nó đại diện.

Ví dụ: Map(), filter(), ReduceByKey(), ...

Các phép biến đổi là các hoạt động lười biếng trên RDD trong Apache Spark. Nó tạo ra một hoặc nhiều RDD mới, thực thi khi một Action xảy ra. Do đó, Transformation tạo ra một tập dữ liệu mới từ tập dữ liệu hiện có.

Một số phép biến đổi nhất định có thể được pipelined, đây là một phương pháp tối ưu hóa mà Spark sử dụng để cải thiện hiệu suất của các phép tính. Có hai loại phép biến hình: phép biến hình hẹp (narrow transformation), phép biến hình rộng(wide transformation).

#### - Narrow Transformation

Đây là kết quả của ánh xạ, bộ lọc và sao cho dữ liệu chỉ từ một phân vùng duy nhất, tức là nó tự cung cấp. Một RDD đầu ra có các phân vùng với các bản ghi bắt nguồn từ một phân vùng duy nhất trong RDD mẹ. Chỉ một tập hợp con giới hạn của các phân vùng được sử dụng để tính toán kết quả.

Spark nhóm các phép biến hình thu hẹp dưới dạng một giai đoạn được gọi là pipelining.

<p align="center"> <img src ="https://d2h0cx97tjks2p.cloudfront.net/blogs/wp-content/uploads/sites/2/2017/08/spark-narrow-transformation-1.jpg" />
<p align="center"> Mô hình Narrow Transformation </p>

#### - Wide Transformation

Đây là kết quả của các hàm như groupByKey() và ReduceByKey(). Dữ liệu cần thiết để tính các bản ghi trong một phân vùng có thể nằm trong nhiều phân vùng của RDD mẹ. Các phép biến đổi rộng còn được gọi là phép biến đổi trộn (shuffle transformations) vì chúng có thể có hoặc không phụ thuộc vào một lần trộn.

<p align="center"> <img src ="https://d2h0cx97tjks2p.cloudfront.net/blogs/wp-content/uploads/sites/2/2017/08/spark-wide-transformation.jpg" />
<p align="center"> Mô hình Wide Transformation </p>

### 2. Actions

Action trong Spark trả về kết quả cuối cùng của các tính toán RDD. Nó kích hoạt thực thi bằng cách sử dụng đồ thị dòng để tải dữ liệu vào RDD ban đầu, thực hiện tất cả các phép biến đổi trung gian và trả về kết quả cuối cùng cho chương trình Driver hoặc ghi nó ra hệ thống tệp. Đồ thị tuyến tính là đồ thị phụ thuộc của tất cả các RDD song song của RDD.

Các Actions là các hoạt động RDD tạo ra các giá trị không phải RDD. Chúng hiện thực hóa một giá trị trong chương trình Spark. Actions là một trong những cách để gửi kết quả từ người thực thi đến driver. First(), take(), Reduce(), collect(), count() là một số Action trong Spark.

Sử dụng các phép biến đổi (Transformations), người ta có thể tạo RDD từ biến hiện có. Nhưng khi chúng ta muốn làm việc với tập dữ liệu thực tế, tại thời điểm đó chúng ta sử dụng Action. Khi Hành động xảy ra, nó không tạo ra RDD mới, không giống như sự chuyển đổi. Do đó, Actions là các hoạt động RDD không cung cấp giá trị RDD. Actions lưu trữ giá trị của nó đối với driver hoặc hệ thống lưu trữ bên ngoài. Nó đưa sự lười biếng (lazy) của RDD vào chuyển động.

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

<p align="center"> <img src ="https://user-images.githubusercontent.com/77916314/106388337-99f29b80-6410-11eb-8678-6249042bde2e.png" />

<p align="center"> <img src ="https://user-images.githubusercontent.com/77916314/106388478-43399180-6411-11eb-9480-78a2afd844ac.png" />


<p align="center"> <img src ="https://user-images.githubusercontent.com/77916314/106388534-8267e280-6411-11eb-8716-81b25446256c.png" />
