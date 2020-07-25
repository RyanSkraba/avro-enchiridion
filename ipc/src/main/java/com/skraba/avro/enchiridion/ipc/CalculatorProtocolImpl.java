package com.skraba.avro.enchiridion.ipc;

/** A simple implementation that can be called by Avro RPC. */
public class CalculatorProtocolImpl implements CalculatorProtocol {
  @Override
  public double add(double augend, double addend) {
    return augend + addend;
  }

  @Override
  public double subtract(double minuend, double subtrahend) {
    return minuend - subtrahend;
  }

  @Override
  public double multiply(double factor1, double factor2) {
    return factor1 * factor2;
  }

  @Override
  public double divide(double dividend, double divisor) {
    return dividend / divisor;
  }

  @Override
  public CharSequence log(double a) {
    System.out.println(a);
    return String.valueOf(a);
  }
}
