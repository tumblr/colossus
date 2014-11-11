module Jekyll
  # Sass plugin to convert .scss to .css
  # 
  # Note: This is configured to use the new css like syntax available in sass.
  require 'sass'
  class SassConverter < Converter
    safe true
    priority :low
 
     def matches(ext)
      ext =~ /scss/i
    end
 
    def output_ext(ext)
      ".css"
    end
 
    def convert(content)
      begin
        puts "Performing Sass Conversion."
        engine = Sass::Engine.new(content, :syntax => :scss, :load_paths => ["./css/", "./_sass/"])
        engine.render
      rescue StandardError => e
        puts "!!! SASS Error: " + e.message
      end
    end
  end
end
