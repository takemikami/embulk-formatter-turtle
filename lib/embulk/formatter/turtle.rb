Embulk::JavaPlugin.register_formatter(
  "turtle", "org.embulk.formatter.turtle.TurtleFormatterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
