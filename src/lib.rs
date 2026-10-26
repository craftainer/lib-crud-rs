//! Generic async CRUD layer for Axum services.
//!
//! The crate is the resource-agnostic half of a CRUD service: everything a
//! host application needs to expose records over HTTP except the records
//! themselves. A host supplies one [`repositories::Repository`]
//! implementation per resource (any storage), and this crate supplies:
//!
//! - [`crud::CrudService`], a thin service over any repository;
//! - [`repositories::filtering`], the storage-agnostic filter and sort
//!   vocabulary, and [`controllers::crud_query`], which parses a query string
//!   into it from a per-resource field table;
//! - [`controllers::crud_actions`], the list-or-get, update and delete
//!   decision logic, including bulk actions with a size cap;
//! - [`controllers::crud_stats`] and [`views::stats`], the building blocks of
//!   stats and prediction endpoints;
//! - [`events`] and [`controllers::crud_events`], a publish/subscribe bus and
//!   its Server-Sent Events stream (the broker-backed bus is behind the
//!   `mqtt` feature);
//! - [`conditional`], entity-tags, `If-None-Match` revalidation and `If-Match`
//!   preconditions;
//! - [`manifest`], the CRUD manifest (contract I1) and the `api-catalog` that
//!   points at it;
//! - [`error::CrudError`], the error type all of the above return, rendered as
//!   RFC 9457 `application/problem+json`.
//!
//! No part of it knows about a concrete resource, an authentication scheme
//! or a database.
//!
//! # Features
//!
//! - `mqtt`: [`events::EventBus::connect`], an MQTT-backed bus with
//!   persistent subscriber sessions. Without it only the in-memory bus
//!   ([`events::EventBus::mock`]) exists.

pub mod conditional;
pub mod controllers;
pub mod crud;
pub mod error;
pub mod events;
pub mod manifest;
pub mod models;
pub mod repositories;
pub mod views;

pub use async_trait;
