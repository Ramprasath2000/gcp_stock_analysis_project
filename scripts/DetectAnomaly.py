class DetectAnomaly(beam.DoFn):
  def __init__(self):
    self.price_history = {}

  def process(self, record):
    symbol = record["symbol"]
    price = record["price"]

    if symbol not in self.price_history:
      self.price_history[symbol]  = []
    history = self.price_history[symbol]

    if len(history) >= 5:
      avg_price = sum(history) / len(history)
      deviation = ((price- avg_price) /avg_price) * 100

    if deviation > 10:
      record["is_anomaly"] = True
      record["anomaly_reason"] = f"SPIKE: {round(deviation,2)}% above average"

    elif deviation < -10:
      record["is_anomaly"] = True
      record["anomaly_reason"] = f"DROP: {round(deviation,2)}% below average"

    history.append(price)
    if len(history) > 10:
      history.pop(0)

    self.price_history[symbol] = history
    yield record
      

    
