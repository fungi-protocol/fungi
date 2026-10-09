//! Change strategy.

use bitcoin::{Amount, FeeRate, TxOut, Weight};
pub mod single_output;

use crate::blockspace::KnowsWeight;
use crate::selection::InputSelection;

/// Change selector may produce outputs but not selected scriptpubkey to along
/// with them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutputWithoutScriptpubkey(Amount);

/// What a `ChangeSelector` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangeSelection {
    selected_change: Vec<OutputWithoutScriptpubkey>,
}

impl ChangeSelection {
    pub(crate) fn new(selected_change: Vec<OutputWithoutScriptpubkey>) -> Self {
        Self { selected_change }
    }
}

/// Change selection strategy.
pub(crate) trait ChangeSelector {
    type Error;

    fn select(
        &self,
        request: InputSelection,
        dust_limit: Amount,
    ) -> Result<ChangeSelection, Self::Error>;
}

/// A change output, with the satisfaction weight of its scriptpubkey.
pub(crate) type OutputWithSatisfactionWeight = (TxOut, Weight);

/// Assigns scriptpubkeys to change outputs.
pub(crate) trait ScriptpubkeyAssigner {
    type Error;

    /// Each change output, with the satisfaction weight of its scriptpubkey.
    fn assign(
        &self,
        change: ChangeSelection,
    ) -> Result<Vec<OutputWithSatisfactionWeight>, Self::Error>;
}

/// Turns an [`InputSelection`]'s surplus into change outputs with scriptpubkeys.
///
/// Each change output pays the fee for its own blockspace out of its value.
pub(crate) struct ChangeStrategy<S, A> {
    selector: S,
    assigner: A,
}

/// Why a [`ChangeStrategy`] produced no change outputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChangeStrategyError<S, A> {
    Selection(S),
    Assignment(A),
    /// A change output's value, less its fee, is below the dust limit.
    BelowDustLimit,
}

impl<S: ChangeSelector, A: ScriptpubkeyAssigner> ChangeStrategy<S, A> {
    pub(crate) fn new(selector: S, assigner: A) -> Self {
        Self { selector, assigner }
    }

    pub(crate) fn change(
        &self,
        request: InputSelection,
        dust_limit: Amount,
        feerate: FeeRate,
    ) -> Result<Vec<OutputWithSatisfactionWeight>, ChangeStrategyError<S::Error, A::Error>> {
        let change = self
            .selector
            .select(request, dust_limit)
            .map_err(ChangeStrategyError::Selection)?;
        let outputs = self
            .assigner
            .assign(change)
            .map_err(ChangeStrategyError::Assignment)?;

        outputs
            .into_iter()
            .map(|(mut output, satisfaction_weight)| {
                output.value = output
                    .fee(feerate)
                    .and_then(|fee| output.value.checked_sub(fee))
                    .filter(|value| *value >= dust_limit)
                    .ok_or(ChangeStrategyError::BelowDustLimit)?;
                Ok((output, satisfaction_weight))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::ScriptBuf;

    #[test]
    fn a_selection_holds_the_change_it_is_built_with() {
        let change = vec![OutputWithoutScriptpubkey(Amount::from_sat(1_000))];

        assert_eq!(ChangeSelection::new(change.clone()).selected_change, change);
    }

    /// Pays every change output to the same scriptpubkey.
    struct SameScriptpubkey(ScriptBuf);

    impl ScriptpubkeyAssigner for SameScriptpubkey {
        type Error = ();

        fn assign(&self, change: ChangeSelection) -> Result<Vec<OutputWithSatisfactionWeight>, ()> {
            Ok((change.selected_change.into_iter())
                .map(|OutputWithoutScriptpubkey(value)| {
                    let output = TxOut {
                        value,
                        script_pubkey: self.0.clone(),
                    };
                    (output, Weight::ZERO)
                })
                .collect())
        }
    }

    fn op_true() -> ScriptBuf {
        ScriptBuf::from_bytes(vec![0x51])
    }

    /// Change paid to [`op_true`], which costs 10 sats to create at 1 sat/vB.
    fn change(
        surplus: u64,
    ) -> Result<Vec<OutputWithSatisfactionWeight>, ChangeStrategyError<(), ()>> {
        let strategy = ChangeStrategy::new(
            single_output::SingleOutputChangeSelector,
            SameScriptpubkey(op_true()),
        );
        let request = InputSelection {
            selected_inputs: vec![],
            surplus: Amount::from_sat(surplus),
        };

        strategy.change(
            request,
            Amount::from_sat(546),
            FeeRate::from_sat_per_vb(1).unwrap(),
        )
    }

    #[test]
    fn a_strategy_assigns_scriptpubkeys_to_the_change_it_selects() {
        assert_eq!(
            change(1_000),
            Ok(vec![(
                TxOut {
                    value: Amount::from_sat(990),
                    script_pubkey: op_true(),
                },
                Weight::ZERO
            )])
        );
    }

    #[test]
    fn change_left_at_the_dust_limit_after_its_fee_is_kept() {
        assert_eq!(
            change(556),
            Ok(vec![(
                TxOut {
                    value: Amount::from_sat(546),
                    script_pubkey: op_true(),
                },
                Weight::ZERO
            )])
        );
    }

    #[test]
    fn change_pushed_below_the_dust_limit_by_its_fee_is_an_error() {
        assert_eq!(change(555), Err(ChangeStrategyError::BelowDustLimit));
    }
}
