// Binfinity MySQL edge addon. Built on the Binfinity Addon SDK — it implements
// ONLY the MySQL convert step (mysqldump/mysql); the SDK owns the universal edge
// contract + BSP data plane. NOT part of the monorepo go.work: addons depend on
// Binfinity only through the SDK + the `binfinity` CLI. See README.md.
module github.com/DiyRex/binfinity-addon-mysql

go 1.25.0

require github.com/DiyRex/binfinity-addon-sdk v0.0.0

// The SDK ships in-repo alongside this addon; resolve it locally. (A published
// addon would pin a real version instead.)
replace github.com/DiyRex/binfinity-addon-sdk => ../..
