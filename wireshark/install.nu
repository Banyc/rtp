let config_dir = $"($env.HOME)/.config/wireshark/plugins/rtp"
let plugin = "dissector.lua"

mkdir --verbose $config_dir
cp --verbose $plugin $config_dir
