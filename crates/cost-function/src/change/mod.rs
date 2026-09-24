//! Change strategy.

use bitcoin::{Amount, FeeRate, Weight};
pub mod single_output;

use crate::blockspace::KnowsWeight;
use crate::selection::InputSelection;

/// The values of the change outputs a `ChangeSelector` decided to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangeSelection(pub(crate) Vec<Amount>);

/// Change selection strategy.
pub(crate) trait ChangeSelector {
    type Error;

    fn select(&self, request: &InputSelection) -> Result<ChangeSelection, Self::Error>;
}

/// The wallet's source of change scriptpubkeys.
///
/// Reports what a change output will cost before any scriptpubkey is assigned, so change can
/// be priced without deriving an address for a transaction that may never be broadcast.
pub(crate) trait ScriptpubkeyAssigner {
    /// Weight a change output adds to a transaction.
    fn output_weight(&self) -> Weight;

    /// Weight of the scriptSig and witness that will spend a change output.
    fn satisfaction_weight(&self) -> Weight;

    /// Smallest value relay policy accepts for a change output.
    fn dust(&self) -> Amount;
}

/// A change output, with everything but its scriptpubkey.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutputWithoutScriptpubkey {
    pub(crate) value: Amount,
    /// Weight the output adds to the transaction.
    pub(crate) weight: Weight,
    /// Weight of the scriptSig and witness that will spend it.
    pub(crate) satisfaction_weight: Weight,
    /// Smallest value relay policy accepts for it.
    pub(crate) dust: Amount,
}

impl KnowsWeight for OutputWithoutScriptpubkey {
    fn weight(&self) -> Weight {
        self.weight
    }
}

/// Turns an [`InputSelection`]'s surplus into change outputs, priced by a
/// [`ScriptpubkeyAssigner`] without assigning scriptpubkeys.
///
/// Each change output pays the fee for its own blockspace out of its value. A change output
/// left below its dust value is dropped, so its value goes to the fee.
pub(crate) struct ChangeStrategy<S, A> {
    selector: S,
    assigner: A,
}

impl<S: ChangeSelector, A: ScriptpubkeyAssigner> ChangeStrategy<S, A> {
    pub(crate) fn new(selector: S, assigner: A) -> Self {
        Self { selector, assigner }
    }

    pub(crate) fn change(
        &self,
        request: &InputSelection,
        feerate: FeeRate,
    ) -> Result<Vec<OutputWithoutScriptpubkey>, S::Error> {
        let ChangeSelection(values) = self.selector.select(request)?;

        Ok(values
            .into_iter()
            .filter_map(|value| {
                let mut output = OutputWithoutScriptpubkey {
                    value,
                    weight: self.assigner.output_weight(),
                    satisfaction_weight: self.assigner.satisfaction_weight(),
                    dust: self.assigner.dust(),
                };
                output.value = output
                    .fee(feerate)
                    .and_then(|fee| value.checked_sub(fee))
                    .filter(|value| *value >= output.dust)?;
                Some(output)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{ScriptBuf, TxOut};
    use std::convert::Infallible;

    fn op_true() -> ScriptBuf {
        ScriptBuf::from_bytes(vec![0x51])
    }

    /// Prices change as if paid to [`op_true`]: 40 WU, so 10 sats to create at 1 sat/vB, a
    /// dust value of 474 sats, and nothing to satisfy.
    struct OpTrue;

    impl ScriptpubkeyAssigner for OpTrue {
        fn output_weight(&self) -> Weight {
            let output = TxOut {
                value: Amount::ZERO,
                script_pubkey: op_true(),
            };
            output.weight()
        }

        fn satisfaction_weight(&self) -> Weight {
            Weight::ZERO
        }

        fn dust(&self) -> Amount {
            op_true().minimal_non_dust()
        }
    }

    /// A change output priced by [`OpTrue`], worth `sats`.
    fn output(sats: u64) -> OutputWithoutScriptpubkey {
        OutputWithoutScriptpubkey {
            value: Amount::from_sat(sats),
            weight: Weight::from_wu(40),
            satisfaction_weight: Weight::ZERO,
            dust: Amount::from_sat(474),
        }
    }

    fn change(surplus: u64) -> Result<Vec<OutputWithoutScriptpubkey>, Infallible> {
        let strategy = ChangeStrategy::new(single_output::SingleOutputChangeSelector, OpTrue);
        let request = InputSelection {
            selected_inputs: vec![],
            surplus: Amount::from_sat(surplus),
        };

        strategy.change(&request, FeeRate::from_sat_per_vb(1).unwrap())
    }

    #[test]
    fn each_change_output_pays_for_its_own_weight() {
        assert_eq!(change(1_000), Ok(vec![output(990)]));
    }

    #[test]
    fn change_left_at_its_dust_value_after_its_fee_is_kept() {
        assert_eq!(change(484), Ok(vec![output(474)]));
    }

    #[test]
    fn change_pushed_below_its_dust_value_by_its_fee_is_dropped() {
        assert_eq!(change(483), Ok(vec![]));
    }
}
