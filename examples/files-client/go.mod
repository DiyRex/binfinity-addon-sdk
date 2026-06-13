// Binfinity files/folder edge addon (example). Built on the Addon SDK; resolves
// it from the repo root via replace.
module github.com/DiyRex/binfinity-addon-files

go 1.25.0

require github.com/DiyRex/binfinity-addon-sdk v0.0.0

replace github.com/DiyRex/binfinity-addon-sdk => ../..
