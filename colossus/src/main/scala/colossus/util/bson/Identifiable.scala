package colossus.util.bson

trait Identifiable[A] {

  def identifier: A

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[Identifiable[A]] && other.asInstanceOf[Identifiable[A]].identifier == this.identifier
  }
}
