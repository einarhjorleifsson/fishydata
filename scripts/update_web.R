# system("quarto preview --render all --no-watch-inputs --no-browse")
# quarto::quarto_render()
system("rm -rf /net/hafri.hafro.is/export/home/hafri/einarhj/public_html/fishydata/*")
system("cp -r _site/* /net/hafri.hafro.is/export/home/hafri/einarhj/public_html/fishydata/.")
system("chmod -R a+rX /net/hafri.hafro.is/export/home/hafri/einarhj/public_html/fishydata")

