package paymentservice

import paymentservice.TestConversions.given
import scala.language.implicitConversions
import munit.FunSuite

import java.time.YearMonth

class CardSuite extends FunSuite {

  private val today = YearMonth.of(2026, 10)

  private def validate(number: String, expiry: String = "12/28") =
    Card.validate(number, expiry, today)

  test("accepts a Luhn-valid visa and derives last4/brand") {
    assertEquals(
      validate("4242424242424242"),
      Right(Card("4242", CardBrand.Visa, "12/28"))
    )
  }

  test("strips spaces before validating") {
    assertEquals(
      validate("4000 0000 0000 0002").map(_.last4),
      Right("0002")
    )
  }

  test("rejects a Luhn-invalid number") {
    assertEquals(validate("4242424242424241"), Left("Invalid card number"))
  }

  test("rejects non-digit characters") {
    assertEquals(validate("4242-4242-4242-4242"), Left("Invalid card number"))
  }

  test("rejects non-ASCII digits that Char.isDigit would accept") {
    // U+0664 ARABIC-INDIC DIGIT FOUR in place of each '4' of a valid number.
    assertEquals(
      validate("4242424242424242".replace('4', '\u0664')),
      Left("Invalid card number")
    )
  }

  test("rejects fewer than 13 or more than 19 digits") {
    // Both Luhn-valid, so only the length rule rejects them.
    assertEquals(validate("424242424242"), Left("Invalid card number"))
    assertEquals(validate("42424242424242424242"), Left("Invalid card number"))
  }

  test("detects brand by prefix") {
    val cases = List(
      "4111111111111111" -> CardBrand.Visa,
      "5555555555554444" -> CardBrand.Mastercard,
      "2223003122003222" -> CardBrand.Mastercard,
      "378282246310005" -> CardBrand.Amex,
      "371449635398431" -> CardBrand.Amex,
      "6011111111111117" -> CardBrand.Other
    )
    cases.foreach { case (number, brand) =>
      assertEquals(validate(number).map(_.brand), Right(brand), number)
    }
  }

  test("treats 2221-2720 as mastercard but not 2220 or 2721") {
    assertEquals(CardBrand.fromNumber("2221000000000009"), CardBrand.Mastercard)
    assertEquals(CardBrand.fromNumber("2720990000000000"), CardBrand.Mastercard)
    assertEquals(CardBrand.fromNumber("2220990000000000"), CardBrand.Other)
    assertEquals(CardBrand.fromNumber("2721000000000000"), CardBrand.Other)
  }

  test("rejects a malformed expiry") {
    List("1228", "13/28", "00/28", "1/28", "12/2028", "ab/cd").foreach { e =>
      assertEquals(
        validate("4242424242424242", e),
        Left("Invalid expiry, expected MM/YY"),
        e
      )
    }
  }

  test("rejects an expiry in the past") {
    assertEquals(
      validate("4242424242424242", "09/26"),
      Left("Card has expired")
    )
  }

  test("accepts an expiry in the current month") {
    assert(validate("4242424242424242", "10/26").isRight)
  }

  test("token is tok_<brand>_<last4>") {
    assertEquals(
      Card("0002", CardBrand.Visa, "12/28").token,
      "tok_visa_0002"
    )
    assertEquals(
      Card("0005", CardBrand.Amex, "12/28").token,
      "tok_amex_0005"
    )
  }

  test("CardBrand.fromString round-trips asString") {
    List(CardBrand.Visa, CardBrand.Mastercard, CardBrand.Amex, CardBrand.Other)
      .foreach(b => assertEquals(CardBrand.fromString(b.asString), Right(b)))
    assert(CardBrand.fromString("diners").isLeft)
  }
}
