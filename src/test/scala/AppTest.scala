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

    val path = this.getClass.getResource("/").getPath
    val jsonParam = "{\"CDP_HOME\":\"" + path + "\"}"

    TaichiApp.main(Array("20170101", "/conf/test_process.json", jsonParam))

  }

}
