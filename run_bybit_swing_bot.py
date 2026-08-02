from bybit_swing.bot import SwingBot, SwingConfig

def main() -> None:
    bot = SwingBot(SwingConfig.load())
    bot.run_forever()

if __name__ == "__main__":
    main()
