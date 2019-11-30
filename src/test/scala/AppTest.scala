import com.captain.bigdata.taichi.TaichiApp

/**
  * AppTest
  *
  * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
  * @date 2017/2/1 16:26
  * @func
  */
object AppTest {

  def main(args: Array[String]): Unit = {

    val args2 = Array[String]("-d", "20190101", "-cf", "./src/test/resources/conf/test_process.json")
    TaichiApp.main(args2)

  }

}
