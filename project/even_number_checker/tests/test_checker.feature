Feature: Check if a number is even

  Scenario: Check even numbers
    Given a number 4
    When I check if the number is even
    Then the result should be true

  Scenario: Check odd numbers
    Given a number 5
    When I check if the number is even
    Then the result should be false
