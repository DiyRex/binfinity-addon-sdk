// Binfinity Addon SDK (Go). A standalone module — stdlib only — that implements
// the UNIVERSAL EDGE CONTRACT once (enroll → heartbeat → poll → execute →
// report) so an addon author writes ONLY the source-specific convert step.
//
// It depends on Binfinity solely through the public contract: HTTP/JSON to the
// Console (CBS/AMS) and the `binfinity` CLI for the BSP data plane. It imports
// nothing from the monorepo, so addons stay decoupled from server internals and
// can be built with GOWORK=off. See README.md and DEVELOPMENT.md.
module github.com/DiyRex/binfinity-addon-sdk

go 1.25.0
