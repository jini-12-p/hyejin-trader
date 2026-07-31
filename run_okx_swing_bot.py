from Okx_swing.bot import SwingBot, SwingConfig


def main() -> None:
    config = SwingConfig.load()
    bot = SwingBot(config)
    bot.run_forever()


if __name__ == "__main__":
    main()
