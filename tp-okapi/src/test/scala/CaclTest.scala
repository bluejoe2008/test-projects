import org.junit.Test

class CaclTest {
  @Test
  def test1(): Unit = {
    val c = new SimpleCalculator();
    println(c.calculate("1+2*(3-1)/4"));
  }
}