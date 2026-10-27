package paymentservice

import cats.effect.Async
import cats.syntax.all._
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter

import scala.concurrent.duration.SECONDS

/** Times a Skunk query, recording a `db.client.operation.duration`
  * measurement tagged with `db.system`/`db.operation` (OTel semantic-convention
  * names), plus `error.type` if it fails. Shared by every Postgres-backed store
  * in this service rather than a new purerest combinator.
  */
trait DbTimer[F[_]] {
  def timed[A](operation: String)(fa: F[A]): F[A]
}

object DbTimer {

  def create[F[_]: Async](meter: Meter[F]): F[DbTimer[F]] =
    meter
      .histogram[Double]("db.client.operation.duration")
      .withUnit("s")
      .create
      .map { histogram =>
        new DbTimer[F] {
          def timed[A](operation: String)(fa: F[A]): F[A] =
            for {
              start <- Async[F].monotonic
              result <- fa.attempt
              end <- Async[F].monotonic
              outcomeAttributes = result match {
                case Right(_)    => Nil
                case Left(error) =>
                  List(Attribute("error.type", error.getClass.getName))
              }
              _ <- histogram.record(
                (end - start).toUnit(SECONDS),
                List(
                  Attribute("db.system", "postgresql"),
                  Attribute("db.operation", operation)
                ) ++ outcomeAttributes
              )
              a <- result.liftTo[F]
            } yield a
        }
      }
}
